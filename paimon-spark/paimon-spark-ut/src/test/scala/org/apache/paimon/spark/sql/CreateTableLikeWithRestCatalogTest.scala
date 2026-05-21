/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.spark.sql

import org.apache.paimon.spark.PaimonSparkTestWithRestCatalogBase

import org.junit.jupiter.api.Assertions

import scala.collection.JavaConverters._

class CreateTableLikeWithRestCatalogTest extends PaimonSparkTestWithRestCatalogBase {

  test("Create table like with REST catalog") {
    assume(gteqSpark3_4)
    withTable("source_tbl", "target_tbl", "target_tbl_qualified") {
      sql("""
            |CREATE TABLE source_tbl (
            |  id BIGINT,
            |  name STRING COMMENT 'name column',
            |  pt STRING
            |) COMMENT 'rest source comment'
            |PARTITIONED BY (pt)
            |TBLPROPERTIES (
            |  'primary-key' = 'id,pt',
            |  'bucket' = '2',
            |  'target-file-size' = '64MB'
            |)
            |""".stripMargin)

      sql("CREATE TABLE target_tbl LIKE source_tbl")
      assertCreatedLike("target_tbl")

      sql(s"CREATE TABLE paimon.$dbName0.target_tbl_qualified LIKE paimon.$dbName0.source_tbl")
      assertCreatedLike("target_tbl_qualified")
    }
  }

  private def assertCreatedLike(tableName: String): Unit = {
    val target = loadTable(tableName)

    Assertions.assertEquals(spark.table("source_tbl").schema, spark.table(tableName).schema)
    Assertions.assertEquals("rest source comment", target.comment().get())
    Assertions.assertEquals(List("pt"), target.partitionKeys().asScala.toList)
    Assertions.assertEquals(List("id", "pt"), target.primaryKeys().asScala.toList)
    Assertions.assertEquals("2", target.options().get("bucket"))
    Assertions.assertEquals("64MB", target.options().get("target-file-size"))
  }
}
