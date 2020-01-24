
package org.apache.spark.sql

import java.io.Closeable
import java.util.concurrent.TimeUnit._
import java.util.concurrent.atomic.AtomicReference

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe.TypeTag
import scala.util.control.NonFatal

import org.apache.spark.{SPARK_VERSION, SparkConf, SparkContext, TaskContext}
import org.apache.spark.annotation.{DeveloperApi, Experimental, Stable, Unstable}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.catalog.Catalog
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, Range}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.internal._
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.SparkSession._
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.util.ExecutionListenerManager
import org.apache.spark.util.{CallSite, Utils}


/**
 * The entry point to programming Spark with the Dataset and DataFrame API.
 *
 * In environments that this has been created upfront (e.g. REPL, notebooks), use the builder
 * to get an existing session:
 *
 * {{{
 *   SecureSparkSession.builder().getOrCreate()
 * }}}
 *
 * The builder can also be used to create a new session:
 *
 * {{{
 *   SecureSparkSession.builder
 *     .master("local")
 *     .appName("Word Count")
 *     .config("spark.some.config.option", "some-value")
 *     .getOrCreate()
 *}}}


 * @param sparkContext The Spark context associated with this Spark session.
 * @param existingSharedState If supplied, use the existing shared state
 *                            instead of creating a new one.
 * @param parentSessionState If supplied, inherit all session state (i.e. temporary
 *                            views, SQL config, UDFs etc) from parent.
 */
@Stable
class SecureSparkSession private(
  
    @transient override val sparkContext: SparkContext,
    @transient private val existingSharedState: SharedState,
    @transient private val parentSessionState: SessionState,
    @transient override private[sql] val extensions: SparkSessionExtensions,
    val userName : String,
    val password : String)
extends SparkSession(sparkContext){ self =>

      def secureRead : SecureDataFrameReader = new SecureDataFrameReader(self)
  }

@Stable
object SecureSparkSession {
  def getSecureSparkSession(userName : String, password : String) : SecureSparkSession ={

    var spark = SparkSession.builder()
          .appName("Secure Spark SQL basic example")
          .config("spark.some.config.option", "some-value")
          .getOrCreate()


    
    val secureSpark : SecureSparkSession = new SecureSparkSession(spark.sparkContext,spark.sharedState,spark.sessionState,spark.extensions,userName, password)
    
    secureSpark

  }
}
