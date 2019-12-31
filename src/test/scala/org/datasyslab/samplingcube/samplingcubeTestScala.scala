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

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SaveMode
import org.apache.spark.storage.StorageLevel
import org.datasyslab.samplingcube.cubes.{CubeLoader, SamplingCube, SamplingIcebergCube, Tabula}
import org.datasyslab.samplingcube.datapreparation.PrepTaxiData
import org.datasyslab.samplingcube.utils.SimplePoint

class samplingcubeTestScala extends testSettings {
  var rawTableName = "inputdf"
  var sampleBudget = 10
  var sampledAttribute = "pickup"
  var qualityAttribute = "pickup"
  var icebergThresholds = Seq(0.005, 0.005)
  var cubedAttributes = Seq("vendor_name", "Passenger_Count")

  describe("Sampling cube builder test") {

    it("Passed SamplingCube full pipeline") {
      var inputDf = spark.read.format("csv").option("delimiter", ",").option("header", "false").load(nyctaxiInputLocation).limit(10000)
      //      var inputDf = spark.read.format("csv").option("delimiter", ",").option("header", "false").load("/hdd/data/nyc-geometry/yellow_tripdata_2009-01_geometry.csv")
      val dataprep = new PrepTaxiData
      dataprep.cubeAttributes = cubedAttributes
      inputDf = dataprep.prep(inputDf, sampledAttribute, qualityAttribute,predicateDfLocation, true)
      dataprep.totalCount = inputDf.count()

      var cubeFactory = new SamplingCube(spark, rawTableName, dataprep.totalCount)
      inputDf.createOrReplaceTempView(rawTableName)

      val twoTables = cubeFactory.buildCube(dataprep.cubeAttributes, sampledAttribute, qualityAttribute, icebergThresholds, dataprep.payload)
      twoTables._1.write.mode(SaveMode.Overwrite).option("header", "true").csv(cubeTableOutputLocation)
      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      val outPutPath = new Path(globalSamTableOutputLocation)
      if (fs.exists(outPutPath)) fs.delete(outPutPath, true)
      twoTables._2.saveAsObjectFile(globalSamTableOutputLocation)

      var cubeDf = spark.read.format("csv").option("delimiter", ",").option("header", "true").load(cubeTableOutputLocation).persist(StorageLevel.MEMORY_AND_DISK_SER)
      var globalSamRdd = spark.sparkContext.objectFile[SimplePoint](globalSamTableOutputLocation)

      println(cubeFactory.globalSample.deep.mkString(", "))
      println("all cells: " + twoTables._1.count())
      cubeDf.show()

      var predicates = dataprep.queryPredicateDf.take(10)(0).toSeq.map(f => f.asInstanceOf[String])
      val cubeLoader = new CubeLoader(Seq(cubeDf), globalSamRdd)
      var sample = cubeLoader.searchCube(cubedAttributes, predicates)._2
      println(sample)
    }

    it("Passed SamplingIcebergCube full pipeline") {
      var inputDf = spark.read.format("csv").option("delimiter", ",").option("header", "false").load(nyctaxiInputLocation)
      //      var inputDf = spark.read.format("csv").option("delimiter", ",").option("header", "false").load("/hdd/data/nyc-geometry/yellow_tripdata_2009-01_geometry.csv")
      val dataprep = new PrepTaxiData
      dataprep.cubeAttributes = dataprep.cubeAttributes
      inputDf = dataprep.prep(inputDf, sampledAttribute, qualityAttribute,predicateDfLocation, true).limit(10000)
      dataprep.totalCount = inputDf.count()

      var cubeFactory = new SamplingIcebergCube(spark, rawTableName, dataprep.totalCount)
      inputDf.createOrReplaceTempView(rawTableName)

      val twoTables = cubeFactory.buildCube(dataprep.cubeAttributes, sampledAttribute, qualityAttribute, icebergThresholds, dataprep.payload)
      twoTables._1.write.mode(SaveMode.Overwrite).option("header", "true").csv(cubeTableOutputLocation)
      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      val outPutPath = new Path(globalSamTableOutputLocation)
      if (fs.exists(outPutPath)) fs.delete(outPutPath, true)
      twoTables._2.saveAsObjectFile(globalSamTableOutputLocation)

      var cubeDf = spark.read.format("csv").option("delimiter", ",").option("header", "true").load(cubeTableOutputLocation).persist(StorageLevel.MEMORY_AND_DISK_SER)
      var globalSamRdd = spark.sparkContext.objectFile[SimplePoint](globalSamTableOutputLocation)

      println(cubeFactory.globalSample.deep.mkString(", "))
      println("all cells: " + cubeDf.count())
      cubeDf.show()

      var predicates = dataprep.queryPredicateDf.take(10)(0).toSeq.map(f => f.asInstanceOf[String])
      val cubeLoader = new CubeLoader(Seq(cubeDf), globalSamRdd)
      var sample = cubeLoader.searchCube(cubedAttributes, predicates)._2
      println(sample)
    }

    it("Passed Tabula full pipeline") {
      var inputDf = spark.read.format("csv").option("delimiter", ",").option("header", "false").load(nyctaxiInputLocation)
      //      var inputDf = spark.read.format("csv").option("delimiter", ",").option("header", "false").load("/hdd/data/nyc-geometry/yellow_tripdata_2009-01_geometry.csv")
      val dataprep = new PrepTaxiData
      dataprep.cubeAttributes = dataprep.cubeAttributes
      inputDf = dataprep.prep(inputDf, sampledAttribute, qualityAttribute, predicateDfLocation, true).persist(StorageLevel.MEMORY_AND_DISK_SER)
      dataprep.totalCount = inputDf.count()

      var cubeFactory = new Tabula(spark, rawTableName, dataprep.totalCount)
      inputDf.createOrReplaceTempView(rawTableName)

      var threeTables = cubeFactory.buildCube(dataprep.cubeAttributes, sampledAttribute, qualityAttribute, icebergThresholds, cubeTableOutputLocation, dataprep.queryPredicateDf, dataprep.payload)
      threeTables._1.write.mode(SaveMode.Overwrite).option("header", "true").csv(cubeTableOutputLocation)
      threeTables._2.write.mode(SaveMode.Overwrite).option("header", "true").csv(sampleTableOutputLocation)
      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      val outPutPath = new Path(globalSamTableOutputLocation)
      if (fs.exists(outPutPath)) fs.delete(outPutPath, true)
      threeTables._3.saveAsObjectFile(globalSamTableOutputLocation)

      var cubeDf = spark.read.format("csv").option("delimiter", ",").option("header", "true").load(cubeTableOutputLocation).persist(StorageLevel.MEMORY_AND_DISK_SER)
      var sampleDf = spark.read.format("csv").option("delimiter", ",").option("header", "true").load(sampleTableOutputLocation).persist(StorageLevel.MEMORY_AND_DISK_SER)
      var globalSamRdd = spark.sparkContext.objectFile[SimplePoint](globalSamTableOutputLocation)

      //println(cubeFactory.globalSample.deep.mkString(", "))
      println("iceberg cells percent " + cubeDf.count() * 1.0 / dataprep.totalPredicateCount + " total cells " + dataprep.totalPredicateCount + " numSamples " + sampleDf.count())
      cubeDf.show()
      sampleDf.show()
      var predicates = dataprep.queryPredicateDf.take(10)(3).toSeq.map(f => f.asInstanceOf[String])

      val cubeLoader = new CubeLoader(Seq(cubeDf, sampleDf), globalSamRdd)
      var sample = cubeLoader.searchCube(dataprep.cubeAttributes, predicates)
    }

    it("Passed Tabula euclidean") {
      var inputDf = spark.read.format("csv").option("delimiter", ",").option("header", "false").load(nyctaxiInputLocation)
      //      var inputDf = spark.read.format("csv").option("delimiter", ",").option("header", "false").load("/hdd/data/nyc-geometry/yellow_tripdata_2009-01_geometry.csv")
      val dataprep = new PrepTaxiData
      dataprep.cubeAttributes = dataprep.cubeAttributes
      inputDf = dataprep.prep(inputDf, sampledAttribute, qualityAttribute, predicateDfLocation, true)
      dataprep.totalCount = inputDf.count()

      inputDf.createOrReplaceTempView(rawTableName)

      var cubeFactory = new Tabula(spark, rawTableName, dataprep.totalCount)
      cubeFactory.drawGlobalSample(sampledAttribute, qualityAttribute, icebergThresholds(0))
      cubeFactory.dryrunWithEuclidean(dataprep.cubeAttributes, sampledAttribute, qualityAttribute, icebergThresholds)

    }
  }
}
