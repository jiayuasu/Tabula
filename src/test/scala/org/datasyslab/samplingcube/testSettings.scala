/*
 * Copyright 2019 Jia Yu (jiayu2@asu.edu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.datasyslab.samplingcube

import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import org.datasyslab.samplingcube.utils.CommonFunctions
import org.scalatest.{BeforeAndAfterAll, FunSpec}

trait testSettings extends FunSpec with BeforeAndAfterAll with CommonFunctions{
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("org.datasyslab.geospark").setLevel(Level.WARN)
  Logger.getLogger("org.datasyslab.samplingcube").setLevel(Level.INFO)

  val resourceFolder = System.getProperty("user.dir") + "/src/test/resources/"
  val nyctaxiInputLocation = resourceFolder + "taxidata-small.csv"
  //val nyctaxiInputLocation = "/hdd/data/nyc-geometry-nonull/yellow_tripdata_2009*.csv"
  val cubeTableOutputLocation = System.getProperty("user.dir") + "/target/cubetable"
  val sampleTableOutputLocation = System.getProperty("user.dir") + "/target/sampletable"
  val fligtInputLocation = resourceFolder + "flights-small.csv"
  val tpchLineItemLocation = resourceFolder + "tpch-lineitem-small.tbl"
  val workloadSize = 10
  val numCubedAttributes = 4
  val predicateDfLocation = System.getProperty("user.dir") + "/target/predicatedf"
  //val fligtInputLocation = "/hdd2/data/flights.csv"
  var spark: SparkSession = SparkSession.builder().config("spark.serializer", classOf[KryoSerializer].getName)
    .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
    .config("geospark.global.index", "true")
    .config("geospark.global.indextype", "quadtree")
    .config("geospark.join.gridtype", "kdbtree")
    .master("local[*]").appName("cubetestScala").getOrCreate()
  GeoSparkSQLRegistrator.registerAll(spark)

  override def beforeAll(): Unit = {
    GeoSparkSQLRegistrator.registerAll(spark)
  }

  override def afterAll(): Unit = {
    //GeoSparkSQLRegistrator.dropAll(spark)
    //spark.stop
  }

  // The utils to measure the function execution time
  def time[T](str: String)(thunk: => T): T = {
    print(str + "... ")
    val t1 = System.currentTimeMillis
    val x = thunk
    val t2 = System.currentTimeMillis
    println((t2 - t1) + " msecs")
    x
  }
}
