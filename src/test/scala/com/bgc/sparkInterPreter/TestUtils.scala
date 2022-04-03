package com.bgc.sparkInterPreter

import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * This TestUtils provide helper methods
 */
trait TestUtils {
  def arrayField[T](items: T*)(implicit m:ClassTag[T]):mutable.WrappedArray[T]={
    mutable.WrappedArray.make(items.toArray)
  }
}
