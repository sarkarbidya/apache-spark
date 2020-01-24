
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
package org.apache.spark.examples.sql


import org.apache.spark.sql.LabeledRow
import org.apache.spark.sql.SecureSparkSession
import org.apache.spark.sql.types._


object SecureSparkSQLExample {

  case class Person(name: String, age: Long)
  
  def main(args: Array[String]): Unit = {
    val secureSpark = SecureSparkSession.getSecureSparkSession("bidya","password")
    import secureSpark.implicits._
    runBasicDataFrameExample(secureSpark)
  }
  
  
  
  private def runBasicDataFrameExample(secureSpark: SecureSparkSession): Unit = {
    val secureDF = secureSpark.secureRead.json("examples/src/main/resources/people.json")
    secureDF.show()
    println("cred details from secure spark : " + secureSpark.userName + " " + secureSpark.password)
    // +----+-------+
    // | age|   name|
    // +----+-------+
    // |null|Michael|
    // |  30|   Andy|
    // |  19| Justin|
    // +----+-------+
    
    import secureSpark.implicits._
    
    val sdf =  secureDF.secureSelect("name").show()
    //sdf.show()
    // +-------+
    // |   name|
    // +-------+
    // |Michael|
    // |   Andy|
    // | Justin|
    // +-------+

    // Select everybody, but increment the age by 1
    secureDF.secureSelect($"name", $"age" + 1).show()
    // +-------+---------+
    // |   name|(age + 1)|
    // +-------+---------+
    // |Michael|     null|
    // |   Andy|       31|
    // | Justin|       20|
    // +-------+---------+

    secureDF.filter($"age" > 41).show()
    // +---+----+
    // |age|name|
    // +---+----+
    // | 30|Andy|
    // +---+----+

    secureDF.SecureGroupBy("age").count().show()



    // +----+-----+
    // | age|count|
    // +----+-----+
    // |  19|    1|
    // |null|    1|
    // |  30|    1|
    // +----+-----+
    // $example off:untyped_ops$
  }

}
