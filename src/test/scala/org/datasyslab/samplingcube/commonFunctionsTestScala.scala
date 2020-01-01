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

import org.apache.spark.sql.functions._
import org.datasyslab.samplingcube.algorithms.FindCombinations
import org.datasyslab.samplingcube.datapreparation.PrepTaxiData
import org.datasyslab.samplingcube.utils.{CommonFunctions, SimplePoint}

import scala.util.Random

class commonFunctionsTestScala extends testSettings with CommonFunctions {

  var rawTableName = "inputdf"
  var sampleBudget = 100
  var sampledAttribute = "pickup"
  var icebergThresholds = Seq(0.1, 0.1)

  describe("CommonFunctions function test") {

    var inputDf = spark.read.format("csv").option("delimiter", ",").option("header", "false").load(nyctaxiInputLocation)
    //      var inputDf = spark.read.format("csv").option("delimiter", ",").option("header", "false").load("/hdd/data/nyc-geometry/yellow_tripdata_2009-01_geometry.csv")
    val dataprep = new PrepTaxiData
    dataprep.cubeAttributes = dataprep.cubeAttributes.take(numCubedAttributes)
    inputDf = dataprep.prep(inputDf, sampledAttribute, predicateDfLocation, false)
    inputDf.createOrReplaceTempView("CommonFunctionsTable")

    inputDf.show()
    dataprep.totalCount = inputDf.count()

    var predicates = dataprep.queryPredicateDf.take(10)(2).toSeq.map(f => f.asInstanceOf[String])

    it(s"Passed calculateFinalLoss") {
      println("calculateFinalLoss " + calculateFinalLoss(inputDf, dataprep.cubeAttributes, predicates, sampledAttribute, "1.0 1.0,1.0 1.0,5.0 5.0,9.0 9.0,9.0 9.0"))
    }

    it(s"Passed generateLossConditionString") {
      println(generateLossConditionString(cubeLocalMeasureName(0), "1 1,2 2,3 3,4 4,5 5", icebergThresholds(0), ">"))
    }

    it(s"Passed generateSamplingFunction") {
      println(generateSamplingFunction(sampledAttribute, sampleBudget))
    }

    it("Passed FindCombinations function") {
      val arr = Array("D", "C", "M", "A")
      val r = 3
      val n = arr.length
      assert(FindCombinations.find(arr, n, r).contains("D,C,M"))
    }

    it(s"Passed sampling_1D") {
      var inputData = Seq[Double]()
      val random = new Random
      for (i <- 1 to 1000) {
        inputData = inputData :+ random.nextGaussian()
      }
      var sample:Array[Double] = null
      time("sampling_1D time") {

        sample = sampling_1D_EucOpt(inputData.toArray, 0.1)
        var lossSum = euclideansum_1D(inputData.toArray, sample)
        var actualLoss = lossSum * 1.0 / inputData.length
        println(s"actual loss = $actualLoss")
        println(s"samplesize = ${sample.length}")
      }
    }

    it(s"Passed sampling_spatial") {
      var inputData = Seq[SimplePoint]()
      val random = new Random
      for (i <- 1 to 1000) {
        inputData = inputData :+ new SimplePoint(random.nextGaussian(), random.nextGaussian())
      }
      var sample:Array[SimplePoint] = null
      time("sampling_spatial time") {

        sample = sampling_spatial_EucOpt(inputData.toArray, 0.1, false)
        var lossSum = euclideansum_spatial(inputData.toArray, sample)
        var actualLoss = lossSum * 1.0 / inputData.length
        println(s"actual loss = $actualLoss")
        println(s"samplesize = ${sample.length}")
      }
    }

    it("Passed sampling_Spatial in Df") {
      val sampleDf = inputDf.limit(1000).groupBy("Passenger_Count").agg(expr("CB_Sampling_Spatial(CB_MergeSpatial(pickup), 0.1)"))
      sampleDf.show(false)
    }

    it("Passed sampling_1D in Df") {
      val sampleDf = inputDf.limit(1000).groupBy("Passenger_Count").agg(expr("CB_Sampling_1D(CB_MergeDouble(Fare_Amt), 0.1)"))
      sampleDf.show()
    }

    it("Pass determine a propoer sample size") {
      assert(calSampleSize(100000, 0.01, 0.01) == 20944)
//      println(dataprep.calculateSampleSize(100000, 0.01, 0.01))
    }
  }
}
