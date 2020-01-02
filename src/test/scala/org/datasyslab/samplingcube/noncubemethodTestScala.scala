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

import org.datasyslab.samplingcube.datapreparation.PrepTaxiData
import org.datasyslab.samplingcube.relatedwork.{SampleFirst, SampleLater}

class noncubemethodTestScala extends testSettings {
  describe("Non-cube method test") {

    it("Passed SampleFirst full pipeline") {
      var inputDf = spark.read.format("csv").option("delimiter", ",").option("header", "false").load(nyctaxiInputLocation)
      //      var inputDf = spark.read.format("csv").option("delimiter", ",").option("header", "false").load("/hdd/data/nyc-geometry/yellow_tripdata_2009-01_geometry.csv")


      var rawTableName = "inputdf"
      var sampleBudget = 10
      var sampledAttribute = "pickup"
      var qualityAttribute = "pickup"
      var cubedAttributes = Seq("vendor_name", "Passenger_Count")

      val dataprep = new PrepTaxiData
      dataprep.cubeAttributes = dataprep.cubeAttributes.take(numCubedAttributes)
      inputDf = dataprep.prep(inputDf, sampledAttribute, predicateDfLocation)
      dataprep.totalCount = inputDf.count()

      var factory = new SampleFirst(spark, rawTableName, sampleBudget, dataprep.totalCount)
      inputDf.createOrReplaceTempView(rawTableName)
      factory.build()
      factory.sampleDf.show()
      var predicates = Seq("VTS", "1")
      var sample = factory.search(cubedAttributes, predicates, sampledAttribute)
      println(sample.deep.mkString(", "))
    }

    it("Passed SampleLater full pipeline") {
      var inputDf = spark.read.format("csv").option("delimiter", ",").option("header", "false").load(nyctaxiInputLocation)
      //      var inputDf = spark.read.format("csv").option("delimiter", ",").option("header", "false").load("/hdd/data/nyc-geometry/yellow_tripdata_2009-01_geometry.csv")

      var rawTableName = "inputdf"
      var sampleBudget = 10
      var sampledAttribute = "pickup"
      var cubedAttributes = Seq("vendor_name", "Passenger_Count")

      val dataprep = new PrepTaxiData
      dataprep.cubeAttributes = dataprep.cubeAttributes.take(numCubedAttributes)
      inputDf = dataprep.prep(inputDf, sampledAttribute, predicateDfLocation)
      dataprep.totalCount = inputDf.count()

      var factory = new SampleLater(spark, rawTableName, sampleBudget)
      inputDf.createOrReplaceTempView(rawTableName)
      var predicates = Seq("VTS", "1")
      var sample = factory.search(cubedAttributes, predicates, sampledAttribute, 0.01)
      println(sample.deep.mkString(", "))
    }
  }
}
