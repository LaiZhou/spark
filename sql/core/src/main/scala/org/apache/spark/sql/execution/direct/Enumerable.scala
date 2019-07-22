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

trait Enumerator[T] {

  /**
   * Advances the iterator to the next tuple. Returns true if there is at least one more tuple.
   */
  def moveNext(): Boolean

  /**
   * Returns the current tuple.
   */
  def current(): T

  /**
   * Sets the enumerator to its initial position, which is before the first
   * element in the collection.
   */
  def reset(): Unit

  /**
   * Closes the iterator and releases all resources. It should be idempotent.
   *
   * Implementations of this must also call the `close()` function of its children.
   */
  def close(): Unit

}

class EnumeratorIterator[T](enumerator: Enumerator[T]) extends Iterator[T] with AutoCloseable {

  var hasNextFlag: Boolean = enumerator.moveNext()

  override def hasNext: Boolean = { hasNextFlag }

  override def next(): T = {
    val t: T = enumerator.current()
    hasNextFlag = enumerator.moveNext()
    t
  }

  override def close(): Unit = {
    enumerator.close()
  }

}
trait Enumerable[T] {

  /**
   * Returns an enumerator that iterates through a collection.
   */
  def enumerator(): Enumerator[T]

  def iterator(): Iterator[T] = new EnumeratorIterator[T](enumerator())

}

class IterableEnumerator[T](iterable: Iterable[T]) extends Enumerator[T] {

  var iterator: Iterator[T] = iterable.iterator
  val DUMMY: Object = new Object()
  var currentElement: T = DUMMY.asInstanceOf[T]

  override def moveNext(): Boolean = {
    if (iterator.hasNext) {
      currentElement = iterator.next()
      true
    } else {
      currentElement = DUMMY.asInstanceOf[T]
      false
    }

  }

  override def current(): T = {
    if (currentElement == DUMMY) {
      throw new NoSuchElementException()
    }
    currentElement
  }

  override def reset(): Unit = {
    iterator = iterable.iterator
    currentElement = DUMMY.asInstanceOf[T]
  }

  override def close(): Unit = {
    iterator match {
      case c: AutoCloseable =>
        try {
          c.close()
        } catch {
          case e: RuntimeException =>
            throw e
          case e: Exception =>
            throw new RuntimeException(e)
        }
      case _ =>
    }
    iterator = null
  }

}
