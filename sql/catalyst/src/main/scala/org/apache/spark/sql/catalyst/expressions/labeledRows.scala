
package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.LabeledRow
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}




class LabeledGenericRow(protected[sql] val values: Array[Any]) extends LabeledRow {
  /** No-arg constructor for serialization. */
  protected def this() = this(null)

  def this(size: Int) = this(new Array[Any](size))

  override def length: Int = values.length

  override def get(i: Int): Any = values(i)

  override def toSeq: Seq[Any] = values.clone()

  override def copy(): LabeledGenericRow = this
}

class LabeledGenericRowWithSchema(values: Array[Any], override val schema: StructType)
  extends LabeledGenericRow(values) {

  /** No-arg constructor for serialization. */
  protected def this() = this(null, null)

  override def fieldIndex(name: String): Int = schema.fieldIndex(name)
}

