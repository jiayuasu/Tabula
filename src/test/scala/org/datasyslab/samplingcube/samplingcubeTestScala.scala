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

import org.apache.spark.sql.SaveMode
import org.apache.spark.storage.StorageLevel
import org.datasyslab.samplingcube.cubes.{SamplingCube, SamplingIcebergCube, Tabula}
import org.datasyslab.samplingcube.datapreparation.PrepTaxiData

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
      dataprep.cubeAttributes = dataprep.cubeAttributes.take(numCubedAttributes)
      inputDf = dataprep.prep(inputDf, sampledAttribute, qualityAttribute,predicateDfLocation, true)
      dataprep.totalCount = inputDf.count()

      var cubeFactory = new SamplingCube(spark, rawTableName, dataprep.totalCount)
      inputDf.createOrReplaceTempView(rawTableName)

      val cubeDf = cubeFactory.buildCube(cubedAttributes, sampledAttribute, qualityAttribute, icebergThresholds, dataprep.payload)
      println(cubeFactory.globalSample.deep.mkString(", "))
      println("all cells: " + cubeDf.count())
      cubeDf.show()

      var predicates = dataprep.queryPredicateDf.take(10)(0).toSeq.map(f => f.asInstanceOf[String])
      var sample = cubeFactory.searchCube(cubeDf, cubedAttributes, predicates)
      println(sample)
    }

    it("Passed SamplingIcebergCube full pipeline") {
      var inputDf = spark.read.format("csv").option("delimiter", ",").option("header", "false").load(nyctaxiInputLocation)
      //      var inputDf = spark.read.format("csv").option("delimiter", ",").option("header", "false").load("/hdd/data/nyc-geometry/yellow_tripdata_2009-01_geometry.csv")
      val dataprep = new PrepTaxiData
      dataprep.cubeAttributes = dataprep.cubeAttributes.take(numCubedAttributes)
      inputDf = dataprep.prep(inputDf, sampledAttribute, qualityAttribute,predicateDfLocation, true).limit(10000)
      dataprep.totalCount = inputDf.count()

      var cubeFactory = new SamplingIcebergCube(spark, rawTableName, dataprep.totalCount)
      inputDf.createOrReplaceTempView(rawTableName)

      val cubeDf = cubeFactory.buildCube(cubedAttributes, sampledAttribute, qualityAttribute, icebergThresholds, dataprep.payload)
      println(cubeFactory.globalSample.deep.mkString(", "))
      println("iceberg cells: " + cubeDf.count())
      cubeDf.show()

      var predicates = dataprep.queryPredicateDf.take(10)(0).toSeq.map(f => f.asInstanceOf[String])
      var sample = cubeFactory.searchCube(cubeDf, cubedAttributes, predicates)
      println(sample)
    }

    it("Passed Tabula full pipeline") {
      var inputDf = spark.read.format("csv").option("delimiter", ",").option("header", "false").load(nyctaxiInputLocation)
      //      var inputDf = spark.read.format("csv").option("delimiter", ",").option("header", "false").load("/hdd/data/nyc-geometry/yellow_tripdata_2009-01_geometry.csv")
      val dataprep = new PrepTaxiData
      dataprep.cubeAttributes = dataprep.cubeAttributes.take(numCubedAttributes)
      inputDf = dataprep.prep(inputDf, sampledAttribute, qualityAttribute, predicateDfLocation, true).persist(StorageLevel.MEMORY_AND_DISK_SER)
      dataprep.totalCount = inputDf.count()

      var cubeFactory = new Tabula(spark, rawTableName, dataprep.totalCount)
      inputDf.createOrReplaceTempView(rawTableName)

      var twoTables = cubeFactory.buildCube(dataprep.cubeAttributes, sampledAttribute, qualityAttribute, icebergThresholds, cubeTableOutputLocation, dataprep.queryPredicateDf, dataprep.payload)
      twoTables._1.write.mode(SaveMode.Overwrite).option("header", "true").csv(cubeTableOutputLocation)
      twoTables._2.write.mode(SaveMode.Overwrite).option("header", "true").csv(sampleTableOutputLocation)

      var cubeDf = spark.read.format("csv").option("delimiter", ",").option("header", "true").load(cubeTableOutputLocation).persist(StorageLevel.MEMORY_AND_DISK_SER)
      var sampleDf = spark.read.format("csv").option("delimiter", ",").option("header", "true").load(sampleTableOutputLocation).persist(StorageLevel.MEMORY_AND_DISK_SER)

      //println(cubeFactory.globalSample.deep.mkString(", "))
      println("iceberg cells percent " + cubeDf.count() * 1.0 / dataprep.totalPredicateCount + " total cells " + dataprep.totalPredicateCount + " numSamples " + sampleDf.count())
      cubeDf.show()
      sampleDf.show()
      var predicates = dataprep.queryPredicateDf.take(10)(3).toSeq.map(f => f.asInstanceOf[String])
      var sample = cubeFactory.searchCube(cubeDf, dataprep.cubeAttributes, predicates, sampleDf)
    }

    it("Passed Tabula euclidean") {
      var inputDf = spark.read.format("csv").option("delimiter", ",").option("header", "false").load(nyctaxiInputLocation)
      //      var inputDf = spark.read.format("csv").option("delimiter", ",").option("header", "false").load("/hdd/data/nyc-geometry/yellow_tripdata_2009-01_geometry.csv")
      val dataprep = new PrepTaxiData
      dataprep.cubeAttributes = dataprep.cubeAttributes.take(numCubedAttributes)
      inputDf = dataprep.prep(inputDf, sampledAttribute, qualityAttribute, predicateDfLocation, true)
      dataprep.totalCount = inputDf.count()

      inputDf.createOrReplaceTempView(rawTableName)

      var cubeFactory = new Tabula(spark, rawTableName, dataprep.totalCount)
      cubeFactory.drawGlobalSample(sampledAttribute, qualityAttribute, icebergThresholds(0))
      cubeFactory.dryrunWithEuclidean(dataprep.cubeAttributes, sampledAttribute, qualityAttribute, icebergThresholds)

    }
  }
}
