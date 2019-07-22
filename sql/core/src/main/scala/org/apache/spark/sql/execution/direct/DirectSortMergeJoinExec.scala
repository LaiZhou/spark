///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.apache.spark.sql.execution.direct
//
//import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
//import org.apache.spark.sql.catalyst.plans.{ExistenceJoin, FullOuter, InnerLike, JoinType, LeftExistence, LeftOuter, RightOuter}
//import org.apache.spark.sql.Row
//import org.apache.spark.sql.catalyst.InternalRow
//
//case class DirectSortMergeJoinExec(
//    leftKeys: Seq[Expression],
//    rightKeys: Seq[Expression],
//    joinType: JoinType,
//    condition: Option[Expression],
//    left: DirectPlan,
//    right: DirectPlan)
//    extends BinaryDirectExecNode {
//
//  override def output: Seq[Attribute] = {
//    joinType match {
//      case _: InnerLike =>
//        left.output ++ right.output
//      case LeftOuter =>
//        left.output ++ right.output.map(_.withNullability(true))
//      case RightOuter =>
//        left.output.map(_.withNullability(true)) ++ right.output
//      case FullOuter =>
//        (left.output ++ right.output).map(_.withNullability(true))
//      case j: ExistenceJoin =>
//        left.output :+ j.exists
//      case LeftExistence(_) =>
//        left.output
//      case x =>
//        throw new IllegalArgumentException(
//          s"${getClass.getSimpleName} should not take $x as the JoinType")
//    }
//  }
//
//  override def collect(): Seq[InternalRow] = {
//    null
//  }
//}
