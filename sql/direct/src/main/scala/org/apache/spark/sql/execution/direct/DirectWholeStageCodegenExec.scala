/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.direct

import scala.util.control.NonFatal

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator
import org.apache.spark.sql.execution.{BufferedRowIterator, CodegenSupport, WholeStageCodegenExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

case class DirectInputAdapter(child: DirectPlan) extends UnaryDirectExecNode {
  override def output: Seq[Attribute] = child.output

  override protected def doExecute(): Iterator[InternalRow] = {
    child.execute()
  }

  override def generateTreeString(
      depth: Int,
      lastChildren: Seq[Boolean],
      append: String => Unit,
      verbose: Boolean,
      prefix: String = "",
      addSuffix: Boolean = false,
      maxFields: Int): Unit = {
    child.generateTreeString(
      depth,
      lastChildren,
      append,
      verbose,
      prefix = "",
      addSuffix = false,
      maxFields)
  }

}
case class DirectWholeStageCodegenExec(plan: WholeStageCodegenExec) extends UnaryDirectExecNode {

  override def output: Seq[Attribute] = plan.output

  override def child: DirectPlan = DirectPlanConverter.convertToDirectPlan(plan.child)

  def codegenStageId: Int = plan.codegenStageId

  override protected def doExecute(): Iterator[InternalRow] = {

    val (ctx, cleanedSource) = plan.doCodeGen()
    // try to compile and fallback if it failed
    val (_, maxCodeSize) = try {
      CodeGenerator.compile(cleanedSource)
    } catch {
      case NonFatal(_) if !Utils.isTesting && sqlContext.conf.codegenFallback =>
        // We should already saw the error message
        logWarning(s"Whole-stage codegen disabled for plan (id=$codegenStageId):\n $treeString")
        return child.execute()
    }
    // Check if compiled code has a too large function
    if (maxCodeSize > sqlContext.conf.hugeMethodLimit) {
      logInfo(
        s"Found too long generated codes and JIT optimization might not work: " +
          s"the bytecode size ($maxCodeSize) is above the limit " +
          s"${sqlContext.conf.hugeMethodLimit}, and the whole-stage codegen was disabled " +
          s"for this plan (id=$codegenStageId). To avoid this, you can raise the limit " +
          s"`${SQLConf.WHOLESTAGE_HUGE_METHOD_LIMIT.key}`:\n$treeString")
      return child.execute()
    }

    val references = ctx.references.toArray

    val durationMs = longMetric("pipelineTime", DirectSQLMetrics.createTimingMetric())

    var children = plan.child.children
    var parentPlan = plan.child
    while (children.length == 1 && parentPlan.isInstanceOf[CodegenSupport]) {
      parentPlan = children.head
      children = parentPlan.children
    }

    assert(children.size <= 2, "Up to two input plan can be supported")
    // we need convert input rdd to DirectPlan here
    if (children.size<2) {
      val inputDirectPlan = DirectPlanConverter.convertToDirectPlan(parentPlan)
      val iter = inputDirectPlan.execute()
      val (clazz, _) = CodeGenerator.compile(cleanedSource)
//      var tc = new TestClass()
      val buffer = clazz.generate(references).asInstanceOf[BufferedRowIterator]
      buffer.init(0, Array(iter))
      new Iterator[InternalRow] {
        override def hasNext: Boolean = {
          val v = buffer.hasNext
          if (!v) durationMs += buffer.durationMs()
          v
        }
        override def next: InternalRow = buffer.next()
      }
    } else {
      val leftDirectPlan = DirectPlanConverter.convertToDirectPlan(children.head)
      val rightDirectPlan = DirectPlanConverter.convertToDirectPlan(children(1))
      val leftIter = leftDirectPlan.execute()
      val rightIter = rightDirectPlan.execute()
      val (clazz, _) = CodeGenerator.compile(cleanedSource)
//      var tc = new TestClass()
      val buffer = clazz.generate(references).asInstanceOf[BufferedRowIterator]
      buffer.init(0, Array(leftIter, rightIter))
      new Iterator[InternalRow] {
        override def hasNext: Boolean = {
          val v = buffer.hasNext
          if (!v) durationMs += buffer.durationMs()
          v
        }
        override def next: InternalRow = buffer.next()
      }
    }
  }

  override def generateTreeString(
      depth: Int,
      lastChildren: Seq[Boolean],
      append: String => Unit,
      verbose: Boolean,
      prefix: String = "",
      addSuffix: Boolean = false,
      maxFields: Int): Unit = {
    plan.generateTreeString(
      depth,
      lastChildren,
      append,
      verbose,
      s"*($codegenStageId) ",
      false,
      maxFields)
  }

}
