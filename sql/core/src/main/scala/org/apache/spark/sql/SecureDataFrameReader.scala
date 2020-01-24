
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

package org.apache.spark.sql

import java.util.{Locale, Properties}

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.ObjectMapper

import org.apache.spark.Partition
import org.apache.spark.annotation.Stable
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.csv.{CSVHeaderChecker, CSVOptions, UnivocityParser}
import org.apache.spark.sql.catalyst.expressions.ExprUtils
import org.apache.spark.sql.catalyst.json.{CreateJacksonParser, JacksonParser, JSONOptions}
import org.apache.spark.sql.catalyst.util.FailureSafeParser
import org.apache.spark.sql.connector.catalog.SupportsRead
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.datasources.csv._
import org.apache.spark.sql.execution.datasources.jdbc._
import org.apache.spark.sql.execution.datasources.json.TextInputJsonDataSource
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2Utils}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String

/**
 * Interface used to load a [[Dataset]] from external storage systems (e.g. file systems,
 * key-value stores, etc). Use `SparkSession.read` to access this.
 *
 *
 * @since 1.4.0
 */
@Stable
class SecureDataFrameReader private[sql](secureSparkSession: SecureSparkSession) extends DataFrameReader(secureSparkSession) {

  private var source: String = secureSparkSession.sessionState.conf.defaultDataSourceName

  private var userSpecifiedSchema: Option[StructType] = None

  private val extraOptions = new scala.collection.mutable.HashMap[String, String]
  override def format(source : String) : SecureDataFrameReader = {
    this.source = source
    this
  }

  

  @scala.annotation.varargs
  override def load(paths: String*): SecureDataFrame = {
    if (source.toLowerCase(Locale.ROOT) == DDLUtils.HIVE_PROVIDER) {
      throw new AnalysisException("Hive data source can only be used with tables, you can not " +
        "read files of Hive data source directly.")
    }

    DataSource.lookupDataSourceV2(source, secureSparkSession.sessionState.conf).map { provider =>
      val sessionOptions = DataSourceV2Utils.extractSessionConfigs(
        source = provider, conf = secureSparkSession.sessionState.conf)
      val pathsOption = if (paths.isEmpty) {
        None
      } else {
        val objectMapper = new ObjectMapper()
        Some("paths" -> objectMapper.writeValueAsString(paths.toArray))
      }

      val finalOptions = sessionOptions ++ extraOptions.toMap ++ pathsOption
      val dsOptions = new CaseInsensitiveStringMap(finalOptions.asJava)
      val table = userSpecifiedSchema match {
        case Some(schema) => provider.getTable(dsOptions, schema)
        case _ => provider.getTable(dsOptions)
      }
      import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Implicits._
      table match {
        case _: SupportsRead if table.supports(BATCH_READ) =>
          SecureDataset.ofRows(secureSparkSession, DataSourceV2Relation.create(table, dsOptions))

        case _ => loadV1Source(paths: _*)
      }
    }.getOrElse(loadV1Source(paths: _*)).asInstanceOf[SecureDataFrame]
  }


   private def loadV1Source(paths: String*) = {
    // Code path for data source v1.
    secureSparkSession.baseRelationToDataFrame(
      DataSource.apply(
        secureSparkSession,
        paths = paths,
        userSpecifiedSchema = userSpecifiedSchema,
        className = source,
        options = extraOptions.toMap).resolveRelation()).asInstanceOf[SecureDataFrame]
  }


  override def load(path : String) : SecureDataFrame = {
    option("path",path).load(Seq.empty: _*).asInstanceOf[SecureDataFrame]
  }

  override def json(path: String) : SecureDataFrame = {
    json(Seq(path) : _*)

  }

  @scala.annotation.varargs
  override def json(paths: String*): SecureDataFrame = format("json").load(paths : _*)

}
