/*
 * Copyright (c) 2019 - 2020 Data Systems Lab at Arizona State University
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

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import org.datasyslab.samplingcube.cubes.{CubeLoader, Tabula}
import org.datasyslab.samplingcube.datapreparation.PrepTaxiData
import org.datasyslab.samplingcube.utils.SimplePoint

object Driver extends App {
  // Control the logging file level
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("org.datasyslab.geospark").setLevel(Level.WARN)
  Logger.getLogger("org.datasyslab.samplingcube").setLevel(Level.INFO)

  //
  var spark: SparkSession = SparkSession.builder().config("spark.serializer", classOf[KryoSerializer].getName)
    .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
    .config("geospark.global.index", "true")
    .config("geospark.global.indextype", "quadtree")
    .config("geospark.join.gridtype", "kdbtree")
    .master("local[*]")
    .appName("TabulaDriver").getOrCreate()
  GeoSparkSQLRegistrator.registerAll(spark)

  /**
    * Parameters
    */
  val resourceFolder = System.getProperty("user.dir") + "/src/test/resources/"
  val nyctaxiInputLocation = resourceFolder + "taxidata-small.csv"
  // Cube table
  val cubeTableOutputLocation = System.getProperty("user.dir") + "/target/cubetable"
  val sampleTableOutputLocation = System.getProperty("user.dir") + "/target/sampletable"
  val globalSamTableOutputLocation = System.getProperty("user.dir") + "/target/globalsamtable"

  // Contain all cells in the cube which is derived from the input table
  // This table is stored on disk. It will be used in Tabula cube initialization and cube search
  val predicateDfLocation = System.getProperty("user.dir") + "/target/predicatedf"

  var rawTableName = "input_table"
  var sampledAttribute = "pickup"
  var icebergThreshold: Double = 0.002 // 1% accuracy loss
  var cubedAttributes = Seq("vendor_name", "Passenger_Count")

  /**
    * Execution mode logic
    */
  if (args.length == 0) throw new IllegalArgumentException(s"[${this.getClass.getName}] The number of parameters should be at least 1")
  if (args(0).equalsIgnoreCase("build")) buildTabula()
  else if (args(0).equalsIgnoreCase("search")) searchTabula()
  else throw new IllegalArgumentException(s"[${this.getClass.getName}] The execution mode can only be build or search")

  /**
    * Build Tabula, a partially materialized sampling cube
    */
  def buildTabula(): Unit ={
    // Load data from disk or HDFS
    var inputDf = spark.read.format("csv").option("delimiter", ",").option("header", "false").load(nyctaxiInputLocation)
//    var inputDf = spark.read.format("csv").option("delimiter", ",").option("header", "false").load("/hdd/data/nyc-geometry/yellow_tripdata_2009-01_geometry.csv")

    // The following steps are to clean up the raw data and compute all cube cells
    val dataprep = new PrepTaxiData
    dataprep.cubeAttributes = cubedAttributes
    dataprep.totalCount = inputDf.count()
    inputDf = dataprep.prep(inputDf, sampledAttribute, predicateDfLocation).persist(StorageLevel.MEMORY_AND_DISK_SER)
    inputDf.createOrReplaceTempView(rawTableName)

    // Build Tabula
    var cubeFactory = new Tabula(spark, rawTableName, dataprep.totalCount)
    var threeTables = cubeFactory.buildCube(dataprep.cubeAttributes, sampledAttribute, icebergThreshold, cubeTableOutputLocation, dataprep.queryPredicateDf, "")

    // Store the produce the cube tables on disk or HDFS
    threeTables._1.write.mode(SaveMode.Overwrite).option("header", "true").csv(cubeTableOutputLocation)
    threeTables._2.write.mode(SaveMode.Overwrite).option("header", "true").csv(sampleTableOutputLocation)
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val outPutPath = new Path(globalSamTableOutputLocation)
    if (fs.exists(outPutPath)) fs.delete(outPutPath, true)
    threeTables._3.saveAsObjectFile(globalSamTableOutputLocation)

    println(s"Total number of cells in the complete cube: ${dataprep.totalPredicateCount}")
    println(s"Total number of iceberg cells in Tabula: ${threeTables._1.count()} (${threeTables._1.count() * 100.0 / dataprep.totalPredicateCount}%)")
    println(s"Total number of sample sets materialized in Tabula: ${threeTables._2.count()} (${threeTables._2.count() * 100.0 / dataprep.totalPredicateCount}%)")
  }

  def searchTabula(): Unit = {
    var cubeDf = spark.read.format("csv").option("delimiter", ",").option("header", "true").load(cubeTableOutputLocation).persist(StorageLevel.MEMORY_AND_DISK_SER)
    var sampleDf = spark.read.format("csv").option("delimiter", ",").option("header", "true").load(sampleTableOutputLocation).persist(StorageLevel.MEMORY_AND_DISK_SER)
    var globalSamRdd = spark.sparkContext.objectFile[SimplePoint](globalSamTableOutputLocation)

    cubeDf.show()
    sampleDf.show()
    val cubeLoader = new CubeLoader(Seq(cubeDf, sampleDf), globalSamRdd)
    val result = cubeLoader.searchCube(Seq("vendor_name", "Passenger_Count"), Seq("CMT", "1"))
    println(s"Tabula cube search query: ${result._1}")
    println(s"Tabula cube search result (the returned sample): ${result._2}")
  }
}