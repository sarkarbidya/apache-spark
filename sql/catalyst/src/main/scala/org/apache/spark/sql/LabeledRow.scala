package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.LabeledGenericRow
import scala.collection.mutable.HashMap
import org.apache.spark.sql.types.{ArrayType, BinaryType, DataType, Decimal, MapType, StringType, StructType, UserDefinedType}


object LabeledRow {
  def unapplySeq(labeledRow: LabeledRow): Some[Seq[Any]] = Some(labeledRow.toSeq)
  
  def apply(values: Any*): LabeledRow = new LabeledGenericRow(values.toArray)

  def fromSeq(values: Seq[Any]): LabeledRow = new LabeledGenericRow(values.toArray)

  def fromTuple(tuple: Product): LabeledRow = fromSeq(tuple.productIterator.toSeq)

  
  @deprecated("This method is deprecated and will be removed in future versions.", "3.0.0")
  def merge(labeledRows: LabeledRow*): LabeledRow = {
    // TODO: Improve the performance of this if used in performance critical part.
    new LabeledGenericRow(labeledRows.flatMap(_.toSeq).toArray)
  }
  
  /** Returns an empty row. */
  val empty = apply()
}




trait LabeledRow extends Row{
  
  override def getStruct(i: Int): LabeledRow = getAs[LabeledRow](i)
  override def copy() : LabeledRow
  override def apply(i: Int): Any = get(i)
  override def schema: StructType = null

  def getRowLabel(row : Row): RowLabel = {

      var rowLabelMap :  HashMap[Owner, Reader] = HashMap.empty[Owner,Reader] //= HashMap((rowOwner, rowReaderSet))
      var rowLabelFetched : String = row.getAs[String]("label")
      var rowLabelFetchedList : List[String] =  rowLabelFetched.split(";").map(_.trim).toList 
      rowLabelFetchedList.foreach{ label =>
        
        var rowOwner = Owner(label.split(":")(0))
        var rowReaderSet = Reader(label.split(":")(1).split(",").toSet)
        rowLabelMap += (rowOwner -> rowReaderSet)
      //  var rowLabelMap :  HashMap[Owner, Reader] = HashMap((rowOwner, rowReaderSet))
//        var rowLabel  = new RowLabel(rowLabelMap)
      }


      var rowLabel  = new RowLabel(rowLabelMap)
      //var rowOwner = Owner(rowLabelFetched.split(":")(0))
      //var rowReaderSet = Reader(rowLabelFetched.split(":")(1).split(",").toSet)

      //var rowLabelMap :  HashMap[Owner, Reader] = HashMap((rowOwner, rowReaderSet))
      //var rowLabel  = new RowLabel(rowLabelMap)
      rowLabel
  }
}
