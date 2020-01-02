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

import java.util.Calendar

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SaveMode
import org.apache.spark.storage.StorageLevel
import org.datasyslab.samplingcube.cubes._
import org.datasyslab.samplingcube.datapreparation.PrepTaxiData
import org.datasyslab.samplingcube.relatedwork.{SampleFirst, SampleLater}
import org.datasyslab.samplingcube.utils.SimplePoint

//import org.datasyslab.samplingcube._
class queryworkloadOnTaxiTestScala extends testSettings {
  var rawTableName = "inputdf"
  var sampleBudget = 100
  var sampledAttribute = "pickup"
  var icebergThreshold = 0.005

  describe("Query workload generator on taxi test") {

    it("Passed query workload generation step on SampleFirst") {
      var inputDf = spark.read.format("csv").option("delimiter", ",").option("header", "false").load(nyctaxiInputLocation)
      val dataprep = new PrepTaxiData
      dataprep.cubeAttributes = dataprep.cubeAttributes.take(numCubedAttributes)
      inputDf = dataprep.prep(inputDf, sampledAttribute,predicateDfLocation).persist(StorageLevel.MEMORY_AND_DISK_SER)
      dataprep.queryPredicateDf.show()
      dataprep.totalCount = inputDf.count()

      // Generate approximate 10 queries
      val queryWorkload = dataprep.generateQueryWorkload(workloadSize)

      var factory = new SampleFirst(spark, rawTableName, sampleBudget, dataprep.totalCount)
      inputDf.createOrReplaceTempView(rawTableName)
      factory.build()

      var elapsedTime: Long = 0
      var loss = 0.0
      queryWorkload.foreach(f => {
        var startingTime = Calendar.getInstance().getTimeInMillis
        var sample = factory.search(dataprep.cubeAttributes, f.asInstanceOf[Seq[String]], sampledAttribute)
        var endingTime = Calendar.getInstance().getTimeInMillis
        elapsedTime += endingTime - startingTime
        loss += calculateFinalLoss(inputDf, dataprep.cubeAttributes, f.asInstanceOf[Seq[String]], sampledAttribute, sample)(0)
      })
      var avgtimeInterval = elapsedTime / queryWorkload.size
      println(s"avg search time of ${queryWorkload.size} queries =" + avgtimeInterval + " avg final sample loss = " + loss / queryWorkload.length)
    }

    it("Passed query workload generation step on SampleLater") {
      var inputDf = spark.read.format("csv").option("delimiter", ",").option("header", "false").load(nyctaxiInputLocation)
      val dataprep = new PrepTaxiData
      dataprep.cubeAttributes = dataprep.cubeAttributes.take(numCubedAttributes)
      inputDf = dataprep.prep(inputDf, sampledAttribute, predicateDfLocation).persist(StorageLevel.MEMORY_AND_DISK_SER)
      dataprep.queryPredicateDf.show()
      dataprep.totalCount = inputDf.count()

      // Generate approximate 10 queries
      val queryWorkload = dataprep.generateQueryWorkload(workloadSize)

      var factory = new SampleLater(spark, rawTableName, sampleBudget)
      inputDf.createOrReplaceTempView(rawTableName)

      var elapsedTime: Long = 0
      var loss = 0.0
      queryWorkload.foreach(f => {
        var startingTime = Calendar.getInstance().getTimeInMillis
        var sample = factory.search(dataprep.cubeAttributes, f.asInstanceOf[Seq[String]], sampledAttribute, icebergThreshold)
        var endingTime = Calendar.getInstance().getTimeInMillis
        elapsedTime += endingTime - startingTime
        loss += calculateFinalLoss(inputDf, dataprep.cubeAttributes, f.asInstanceOf[Seq[String]], sampledAttribute, sample)(0)
      })
      var avgtimeInterval = elapsedTime / queryWorkload.size
      println(s"avg search time of ${queryWorkload.size} queries =" + avgtimeInterval + " avg final sample loss = " + loss / queryWorkload.length)
    }

    it("Passed query workload generation step on SamplingCube") {
      var inputDf = spark.read.format("csv").option("delimiter", ",").option("header", "false").load(nyctaxiInputLocation)
      val dataprep = new PrepTaxiData
      dataprep.cubeAttributes = dataprep.cubeAttributes.take(numCubedAttributes)
      inputDf = dataprep.prep(inputDf, sampledAttribute, predicateDfLocation).limit(10000).persist(StorageLevel.MEMORY_AND_DISK_SER)
      dataprep.queryPredicateDf.show()
      dataprep.totalCount = inputDf.count()

      // Generate approximate 10 queries
      val queryWorkload = dataprep.generateQueryWorkload(workloadSize)

      var cubeFactory = new SamplingCube(spark, rawTableName, dataprep.totalCount)
      inputDf.createOrReplaceTempView(rawTableName)

      val twoTables = cubeFactory.buildCube(dataprep.cubeAttributes, sampledAttribute, icebergThreshold, dataprep.payload)
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

      val cubeLoader = new CubeLoader(Seq(cubeDf), globalSamRdd)
      var elapsedTime: Long = 0
      var loss = 0.0
      queryWorkload.foreach(f => {
        var startingTime = Calendar.getInstance().getTimeInMillis
        var sample = cubeLoader.searchCube(dataprep.cubeAttributes, f.asInstanceOf[Seq[String]])._2
        var endingTime = Calendar.getInstance().getTimeInMillis
        elapsedTime += endingTime - startingTime
        loss += calculateFinalLoss(inputDf, dataprep.cubeAttributes, f.asInstanceOf[Seq[String]], sampledAttribute, sample)(0)
      })
      var avgtimeInterval = elapsedTime / queryWorkload.size
      println(s"avg search time of ${queryWorkload.size} queries =" + avgtimeInterval + " avg final sample loss = " + loss / queryWorkload.length)
    }

    it("Passed query workload generation step on SamplingIcebergCube") {
      var inputDf = spark.read.format("csv").option("delimiter", ",").option("header", "false").load(nyctaxiInputLocation)
      val dataprep = new PrepTaxiData
      dataprep.cubeAttributes = dataprep.cubeAttributes.take(numCubedAttributes)
      inputDf = dataprep.prep(inputDf, sampledAttribute, predicateDfLocation).limit(10000).persist(StorageLevel.MEMORY_AND_DISK_SER)
      dataprep.queryPredicateDf.show()
      dataprep.totalCount = inputDf.count()

      // Generate approximate 10 queries
      val queryWorkload = dataprep.generateQueryWorkload(workloadSize)

      var cubeFactory = new SamplingIcebergCube(spark, rawTableName, dataprep.totalCount)
      inputDf.createOrReplaceTempView(rawTableName)
      val twoTables = cubeFactory.buildCube(dataprep.cubeAttributes, sampledAttribute, icebergThreshold, dataprep.payload)
      twoTables._1.write.mode(SaveMode.Overwrite).option("header", "true").csv(cubeTableOutputLocation)
      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      val outPutPath = new Path(globalSamTableOutputLocation)
      if (fs.exists(outPutPath)) fs.delete(outPutPath, true)
      twoTables._2.saveAsObjectFile(globalSamTableOutputLocation)

      var cubeDf = spark.read.format("csv").option("delimiter", ",").option("header", "true").load(cubeTableOutputLocation).persist(StorageLevel.MEMORY_AND_DISK_SER)
      var globalSamRdd = spark.sparkContext.objectFile[SimplePoint](globalSamTableOutputLocation)


      println("iceberg cell percent = " + cubeDf.count() * 1.0 / dataprep.totalPredicateCount + "total cells = " + dataprep.totalPredicateCount)

      val cubeLoader = new CubeLoader(Seq(cubeDf), globalSamRdd)
      var elapsedTime: Long = 0
      var loss = 0.0
      queryWorkload.foreach(f => {
        var startingTime = Calendar.getInstance().getTimeInMillis
        var sample = cubeLoader.searchCube(dataprep.cubeAttributes, f.asInstanceOf[Seq[String]])._2
        var endingTime = Calendar.getInstance().getTimeInMillis
        elapsedTime += endingTime - startingTime
        loss += calculateFinalLoss(inputDf, dataprep.cubeAttributes, f.asInstanceOf[Seq[String]], sampledAttribute, sample)(0)
      })
      var avgtimeInterval = elapsedTime / queryWorkload.size
      println(s"avg search time of ${queryWorkload.size} queries =" + avgtimeInterval + " avg final sample loss = " + loss / queryWorkload.length)
    }

    it("Passed query workload generation step on Tabula") {
      var inputDf = spark.read.format("csv").option("delimiter", ",").option("header", "false").load(nyctaxiInputLocation)
      val dataprep = new PrepTaxiData
      dataprep.cubeAttributes = dataprep.cubeAttributes.take(numCubedAttributes)
      inputDf = dataprep.prep(inputDf, sampledAttribute, predicateDfLocation).limit(10000).persist(StorageLevel.MEMORY_AND_DISK_SER)
      dataprep.queryPredicateDf.show()
      dataprep.totalCount = inputDf.count()

      // Generate approximate 10 queries
      val queryWorkload = dataprep.generateQueryWorkload(workloadSize)

      var cubeFactory = new Tabula(spark, rawTableName, dataprep.totalCount)
      inputDf.createOrReplaceTempView(rawTableName)

      var threeTables = cubeFactory.buildCube(dataprep.cubeAttributes, sampledAttribute, icebergThreshold, cubeTableOutputLocation, dataprep.queryPredicateDf, dataprep.payload)
      threeTables._1.write.mode(SaveMode.Overwrite).option("header", "true").csv(cubeTableOutputLocation)
      threeTables._2.write.mode(SaveMode.Overwrite).option("header", "true").csv(sampleTableOutputLocation)
      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      val outPutPath = new Path(globalSamTableOutputLocation)
      if (fs.exists(outPutPath)) fs.delete(outPutPath, true)
      threeTables._3.saveAsObjectFile(globalSamTableOutputLocation)

      var cubeDf = spark.read.format("csv").option("delimiter", ",").option("header", "true").load(cubeTableOutputLocation).persist(StorageLevel.MEMORY_AND_DISK_SER)
      var sampleDf = spark.read.format("csv").option("delimiter", ",").option("header", "true").load(sampleTableOutputLocation).persist(StorageLevel.MEMORY_AND_DISK_SER)
      var globalSamRdd = spark.sparkContext.objectFile[SimplePoint](globalSamTableOutputLocation)

      val cubeLoader = new CubeLoader(Seq(cubeDf, sampleDf), globalSamRdd)

      println("iceberg cells percent " + cubeDf.count() * 1.0 / dataprep.totalPredicateCount + " total cells " + dataprep.totalPredicateCount + " stored cells percent " + sampleDf.count() * 1.0 / dataprep.totalPredicateCount)
      var elapsedTime: Long = 0
      var loss = 0.0

      queryWorkload.foreach(f => {
        var startingTime = Calendar.getInstance().getTimeInMillis
        var sample = cubeLoader.searchCube(dataprep.cubeAttributes, f.asInstanceOf[Seq[String]])._2
        var endingTime = Calendar.getInstance().getTimeInMillis
        elapsedTime += endingTime - startingTime
        loss += calculateFinalLoss(inputDf, dataprep.cubeAttributes, f.asInstanceOf[Seq[String]], sampledAttribute, sample)(0)
      })
      var avgtimeInterval = elapsedTime / queryWorkload.size
      println(s"avg search time of ${queryWorkload.size} queries =" + avgtimeInterval + " avg final sample loss = " + loss / queryWorkload.length)
    }

    it("Passed query workload generation step on TabulaNoSamS") {
      var inputDf = spark.read.format("csv").option("delimiter", ",").option("header", "false").load(nyctaxiInputLocation)
      val dataprep = new PrepTaxiData
      dataprep.cubeAttributes = dataprep.cubeAttributes.take(numCubedAttributes)
      inputDf = dataprep.prep(inputDf, sampledAttribute, predicateDfLocation).persist(StorageLevel.MEMORY_AND_DISK_SER)
      dataprep.queryPredicateDf.show()
      dataprep.totalCount = inputDf.count()

      // Generate approximate 10 queries
      val queryWorkload = dataprep.generateQueryWorkload(workloadSize)

      var cubeFactory = new TabulaNoSamS(spark, rawTableName, dataprep.totalCount)
      inputDf.createOrReplaceTempView(rawTableName)
      var twoTables = cubeFactory.buildCubeNoSamS(dataprep.cubeAttributes, sampledAttribute, icebergThreshold, cubeTableOutputLocation, dataprep.queryPredicateDf, dataprep.payload)

      twoTables._1.write.mode(SaveMode.Overwrite).option("header", "true").csv(cubeTableOutputLocation)
      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      val outPutPath = new Path(globalSamTableOutputLocation)
      if (fs.exists(outPutPath)) fs.delete(outPutPath, true)
      twoTables._2.saveAsObjectFile(globalSamTableOutputLocation)

      var cubeDf = spark.read.format("csv").option("delimiter", ",").option("header", "true").load(cubeTableOutputLocation).persist(StorageLevel.MEMORY_AND_DISK_SER)
      var globalSamRdd = spark.sparkContext.objectFile[SimplePoint](globalSamTableOutputLocation)

      println("iceberg cell percent = " + cubeDf.count() * 1.0 / dataprep.totalPredicateCount + "total cells = " + dataprep.totalPredicateCount)

      val cubeLoader = new CubeLoader(Seq(cubeDf), globalSamRdd)
      var elapsedTime: Long = 0
      var loss = 0.0
      queryWorkload.foreach(f => {
        var startingTime = Calendar.getInstance().getTimeInMillis
        var sample = cubeLoader.searchCube(dataprep.cubeAttributes, f.asInstanceOf[Seq[String]])._2
        var endingTime = Calendar.getInstance().getTimeInMillis
        elapsedTime += endingTime - startingTime
        if (calculateFinalLoss(inputDf, dataprep.cubeAttributes, f.asInstanceOf[Seq[String]], sampledAttribute, sample)(0)>=0) loss += calculateFinalLoss(inputDf, dataprep.cubeAttributes, f.asInstanceOf[Seq[String]], sampledAttribute, sample)(0)
      })
      var avgtimeInterval = elapsedTime / queryWorkload.size
      println(s"avg search time of ${queryWorkload.size} queries =" + avgtimeInterval + " avg final sample loss = " + loss / queryWorkload.length)
    }
  }
}
