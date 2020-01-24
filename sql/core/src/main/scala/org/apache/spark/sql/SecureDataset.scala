
package org.apache.spark.sql

import java.io.{ByteArrayOutputStream, CharArrayWriter, DataOutputStream}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.util.control.NonFatal

import org.apache.commons.lang3.StringUtils

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.annotation.{DeveloperApi, Stable, Unstable}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.function._
import org.apache.spark.api.python.{PythonRDD, SerDeUtil}
import org.apache.spark.api.r.RRDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.json.{JacksonGenerator, JSONOptions}
import org.apache.spark.sql.catalyst.optimizer.CombineUnions
import org.apache.spark.sql.catalyst.parser.{ParseException, ParserUtils}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, PartitioningCollection}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.catalyst.util.IntervalUtils
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.arrow.{ArrowBatchStreamWriter, ArrowConverters}
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2ScanRelation, FileTable}
import org.apache.spark.sql.execution.python.EvaluatePython
import org.apache.spark.sql.execution.stat.StatFunctions
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.SchemaUtils
import org.apache.spark.storage.StorageLevel
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.unsafe.types.CalendarInterval
import org.apache.spark.util.Utils
//import org.apache.spark.sql.RowLabel

import scala.collection.mutable.HashMap



private[sql] object SecureDataset {
  val curId = new java.util.concurrent.atomic.AtomicLong()
  val DATASET_ID_KEY = "__dataset_id"
  val COL_POS_KEY = "__col_position"
  val DATASET_ID_TAG = TreeNodeTag[Long]("dataset_id")

  def apply[T: Encoder](secureSparkSession: SecureSparkSession, logicalPlan: LogicalPlan): SecureDataset[T] = {
    val secureDataset = new SecureDataset(secureSparkSession, logicalPlan, implicitly[Encoder[T]])
    // Eagerly bind the encoder so we verify that the encoder matches the underlying
    // schema. The user will get an error if this is not the case.
    // optimization: it is guaranteed that [[InternalRow]] can be converted to [[Row]] so
    // do not do this check in that case. this check can be expensive since it requires running
    // the whole [[Analyzer]] to resolve the deserializer
    if (secureDataset.exprEnc.clsTag.runtimeClass != classOf[LabeledRow]) {
      secureDataset.resolvedEnc
    }
    secureDataset
  }

  def ofRows(secureSparkSession: SecureSparkSession, logicalPlan: LogicalPlan): SecureDataFrame = {
    val qe = secureSparkSession.sessionState.executePlan(logicalPlan)
    qe.assertAnalyzed()
    new SecureDataset[Row](secureSparkSession, qe, RowEncoder(qe.analyzed.schema))
  }

  /** A variant of ofRows that allows passing in a tracker so we can track query parsing time. */
  def ofRows(secureSparkSession: SecureSparkSession, logicalPlan: LogicalPlan, tracker: QueryPlanningTracker)
    : SecureDataFrame = {
    val qe = new QueryExecution(secureSparkSession, logicalPlan, tracker)
    qe.assertAnalyzed()
    new SecureDataset[Row](secureSparkSession, qe, RowEncoder(qe.analyzed.schema))
  }
}



class SecureDataset[T] private[sql](
    @transient private val _secureSparkSession: SecureSparkSession,
    @DeveloperApi @Unstable @transient override val queryExecution: QueryExecution,
    @DeveloperApi @Unstable @transient override val encoder: Encoder[T])
  extends Dataset(_secureSparkSession, queryExecution,encoder) {



  @transient lazy val secureSparkSession: SecureSparkSession = {
    if (_secureSparkSession == null) {
      throw new SparkException(
      "Dataset transformations and actions can only be invoked by the driver, not inside of" +
        " other Dataset transformations; for example, dataset1.map(x => dataset2.values.count()" +
        " * x) is invalid because the values transformation and count action cannot be " +
        "performed inside of the dataset1.map transformation. For more information," +
        " see SPARK-28702.")
    }
    _secureSparkSession
  }

 //@transient lazy val secureEncoder = org.apache.spark.sql.catalyst.encoders.ExpressionEncoder[SecureDataset[T]]

  def this(secureSparkSession: SecureSparkSession, logicalPlan: LogicalPlan, encoder: Encoder[T]) = {
    this(secureSparkSession, secureSparkSession.sessionState.executePlan(logicalPlan), encoder)
  }

  def this(sqlContext: SQLContext, logicalPlan: LogicalPlan, encoder: Encoder[T]) = {
    this(sqlContext.sparkSession.asInstanceOf[SecureSparkSession], logicalPlan, encoder)
  }

  @transient override lazy val sqlContext: SQLContext = _secureSparkSession.sqlContext
  override private[sql] implicit val exprEnc: ExpressionEncoder[T] = encoderFor(encoder)
  private lazy val resolvedEnc = {
    exprEnc.resolveAndBind(logicalPlan.output, _secureSparkSession.sessionState.analyzer)
  }

   override def show(numRows: Int): Unit = show(numRows, truncate = false )//true)

  /**
   * Displays the top 20 rows of Dataset in a tabular form. Strings more than 20 characters
   * will be truncated, and all cells will be aligned right.
   *
   * @group action
   * @since 1.6.0
   */
  /**
   *
   * @throws UnsupportedOperationException when label mismatch is found
   */
  override def show(): Unit = show(20)

  /**
   *
   * 
   * @throws UnsupportedOperationException when label mismatch is found
   */

  override def show(truncate: Boolean): Unit = show(20, truncate)
  
  
  override private[sql] def getRows(
      numRows: Int,
      truncate: Int): Seq[Seq[String]] = {
      
      val newDf = toDF()
      
      val castCols = newDf.logicalPlan.output.map { col =>
        if (col.dataType == BinaryType) {
          Column(col)
        } else {
          Column(col).cast(StringType)
        }
      }
      
      val data = newDf.select(castCols: _*).take(numRows + 1)
      var schemaLabel : HashMap[Owner, Reader] = HashMap.empty[Owner,Reader]

      schema.fieldNames.toSeq +: data.map { row =>
        var labeledRow = row match {
          case _ => LabeledRow.apply()
        }
        var rowLabel = labeledRow.getRowLabel(row)
        schemaLabel ++= rowLabel.doUnion(schemaLabel)
      }
      

      var userOwner = Owner(secureSparkSession.userName)
      var userReaderSet = Reader(Set(secureSparkSession.userName))  //Reader("bidya".toSet)
      var userLabel :  HashMap[Owner, Reader] = HashMap((userOwner, userReaderSet))
      var rowLabelValidator  = new RowLabel(schemaLabel)
      
      println("table label union " + schemaLabel)
      println("user label " + userLabel)

      if(rowLabelValidator.isRestriction(userLabel) ){
        println("restriction validated")
        schema.fieldNames.toSeq +: data.map { row =>
          var labeledRow = row match {
            case _ => LabeledRow.apply()
          }
          var rowLabel = labeledRow.getRowLabel(row).rowLabel
          var rowOwnerSet = rowLabel.keySet
            row.toSeq.map { cell =>
              val str = cell match {
                case null => "null"
                case binary: Array[Byte] => binary.map("%02X".format(_)).mkString("[", " ", "]")
                case _ => cell.toString
              }
    
              if (truncate > 0 && str.length > truncate) {
                if (truncate < 4) str.substring(0, truncate)
                else str.substring(0, truncate - 3) + "..."
              } else {
                  str
              }
            }: Seq[String]
        }}else{
           throw new UnsupportedOperationException("illegal access request")
            val str : String = ""
            Seq(Seq(str))
          }
    }
/*
// prev code for restriction

    
    schema.fieldNames.toSeq +: data.map { row  =>
      // actual imp starts here------------------------------------
      var labeledRow = row match {
        case _  => LabeledRow.apply()
      }

      
      var rowLabel =  labeledRow.getRowLabel(row)
      //
      //actual imp ends here --------------------------------------
       //var rowLabelValidator = new RowLabel(rowLabel)
      var userOwner = Owner(secureSparkSession.userName)
      var userReaderSet = Reader(Set(secureSparkSession.userName))  //Reader("bidya".toSet)
      var userLabel :  HashMap[Owner, Reader] = HashMap((userOwner, userReaderSet))
      //println(userLabel)
      // added for temp test starts here --------------------------
      //var rowLabelFetched : String = row.getAs[String]("label")
      //var rowOwner = Owner(rowLabelFetched.split(":")(0))
      //var rowReaderSet = Reader(rowLabelFetched.split(":")(1).split(",").toSet)
      //var rowLabelMap :  HashMap[Owner, Reader] = HashMap((rowOwner, rowReaderSet))
      //var rowLabel  = new RowLabel(rowLabelMap)
  //added for temp test ends here -----------------------------
     if(rowLabel.isRestriction(userLabel) ){
      
        
        row.toSeq.map { cell =>
          val str = cell match {
            case null => "null"
            case binary: Array[Byte] => binary.map("%02X".format(_)).mkString("[", " ", "]")
            case _ => cell.toString
          }
    
          if (truncate > 0 && str.length > truncate) {
            // do not show ellipses for strings shorter than 4 characters.
            if (truncate < 4) str.substring(0, truncate)
            else str.substring(0, truncate - 3) + "..."
          } else {
            str
          }
        }: Seq[String]
      }else{
          
          val str = Seq.empty[String]
          str
          //continue
          
      }
      }*/
    //}
  //}
   


   override def toDF(): SecureDataFrame = new SecureDataset[Row](_secureSparkSession, queryExecution, RowEncoder(schema))


  /** A convenient function to wrap a logical plan and produce a DataFrame. */
  @inline private def withPlan(logicalPlan: LogicalPlan): SecureDataFrame = {
    SecureDataset.ofRows(_secureSparkSession, logicalPlan)
  }.drop("all")

  
  @scala.annotation.varargs
 override  def select(cols: Column*): SecureDataFrame = withPlan {
    Project(cols.map(_.named), logicalPlan)
  }

  
  @scala.annotation.varargs
   def secureSelect(cols: Column*): SecureDataFrame = withPlan {
     Project((cols :+ Column("label")).map(_.named), logicalPlan)
    }
     
     
     
    // select((cols :+ Column("label")).map(Column(_)) : _*)

  @scala.annotation.varargs
   def secureSelect(col: String, cols: String*): SecureDataFrame = secureSelect((col +: cols :+ "label").map(Column(_)) : _*)



  @scala.annotation.varargs
  override def selectExpr(exprs: String*): SecureDataFrame = {
    secureSelect(exprs.map { expr =>
      Column(_secureSparkSession.sessionState.sqlParser.parseExpression(expr))
    }: _*)
  }



  override def map[U : Encoder](func: T => U): SecureDataset[U] = withTypedPlan {
    MapElements[T, U](func, logicalPlan)
  }


  override def map[U](func: MapFunction[T, U], encoder: Encoder[U]): SecureDataset[U] = {
    implicit val uEnc = encoder
    withTypedPlan(MapElements[T, U](func, logicalPlan))
  }

  
  override def mapPartitions[U : Encoder](func: Iterator[T] => Iterator[U]): SecureDataset[U] = {
    new SecureDataset[U](
      _secureSparkSession,
      MapPartitions[T, U](func, logicalPlan),
      implicitly[Encoder[U]])
  }

  override def mapPartitions[U](f: MapPartitionsFunction[T, U], encoder: Encoder[U]): SecureDataset[U] = {
    val func: (Iterator[T]) => Iterator[U] = x => f.call(x.asJava).asScala
    mapPartitions(func)(encoder)
  }


  @inline private def withTypedPlan[U : Encoder](logicalPlan: LogicalPlan): SecureDataset[U] = {
    SecureDataset(_secureSparkSession, logicalPlan)
  }


  override def as(alias: String): SecureDataset[T] = withTypedPlan {
    SubqueryAlias(alias, logicalPlan)
  }

  override def as(alias: Symbol): SecureDataset[T] = as(alias.name)

  /**
   * Returns a new Dataset with an alias set. Same as `as`.
   *
   * @group typedrel
   * @since 2.0.0
   */
  override def alias(alias: String): SecureDataset[T] = as(alias)

  /**
   * (Scala-specific) Returns a new Dataset with an alias set. Same as `as`.
   *
   * @group typedrel
   * @since 2.0.0
   */
  override def alias(alias: Symbol): SecureDataset[T] = as(alias)

  override def filter(condition: Column): SecureDataset[T] = withTypedPlan {
    Filter(condition.expr, logicalPlan)
  }//.drop("all").as(implicitly[Encoder[SecureDataset[T]]])


  override def filter(conditionExpr: String): SecureDataset[T] = {
    filter(Column(_secureSparkSession.sessionState.sqlParser.parseExpression(conditionExpr)))
  }//.drop("all").as(implicitly[Encoder[T]])

  override def filter(func: T => Boolean): SecureDataset[T]= {
    withTypedPlan(TypedFilter(func, logicalPlan))
  }//.drop("all")as(implicitly[Encoder[T]])
  
  override def filter(func: FilterFunction[T]): SecureDataset[T] = {
    withTypedPlan(TypedFilter(func, logicalPlan))
  }//.drop("all")as(implicitly[Encoder[T]])




  @scala.annotation.varargs
  def SecureGroupBy(cols: Column*): SecureRelationalGroupedDataset = {
    SecureRelationalGroupedDataset(toDF(), (cols :+ Column("label")).map(_.expr), SecureRelationalGroupedDataset.GroupByType)
  }

  @scala.annotation.varargs
   def SecureGroupBy(col1: String, cols: String*): SecureRelationalGroupedDataset = {
    val colNames: Seq[String] = col1 +: cols :+ "label"
    SecureRelationalGroupedDataset(
      toDF(), colNames.map(colName => resolve(colName)), SecureRelationalGroupedDataset.GroupByType)
  }





  override def drop(colName: String): SecureDataFrame = {
    drop(Seq(colName) : _*)
  }


  @scala.annotation.varargs
  override def drop(colNames: String*): SecureDataFrame = {
    val resolver = secureSparkSession.sessionState.analyzer.resolver
    val allColumns = queryExecution.analyzed.output
    val remainingCols = allColumns.filter { attribute =>
      colNames.forall(n => !resolver(attribute.name, n))
    }.map(attribute => Column(attribute))
    if (remainingCols.size == allColumns.size) {
      toDF()
    } else {
      this.secureSelect(remainingCols: _*)
    }
  }



  override def drop(col: Column): SecureDataFrame = {
    val expression = col match {
      case Column(u: UnresolvedAttribute) =>
        queryExecution.analyzed.resolveQuoted(
          u.name, secureSparkSession.sessionState.analyzer.resolver).getOrElse(u)
      case Column(expr: Expression) => expr
    }
    val attrs = this.logicalPlan.output
    val colsAfterDrop = attrs.filter { attr =>
      !attr.semanticEquals(expression)
    }.map(attr => Column(attr))
    secureSelect(colsAfterDrop : _*)
  }


}



