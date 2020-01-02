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

import org.datasyslab.samplingcube.datapreparation.{PrepFlightData, PrepTaxiData}

class datapreparationTestScala extends testSettings {

  describe("Data preparation test") {

    it("Passed nyc taxi prep step") {
      var inputDf = spark.read.format("csv").option("delimiter", ",").option("header", "false").load(nyctaxiInputLocation)
      //var inputDf = spark.read.format("csv").option("delimiter", ",").option("header", "false").load("/hdd/data/nyc-geometry-nonull/yellow_tripdata*")
      val dataprep = new PrepTaxiData
      dataprep.cubeAttributes = dataprep.cubeAttributes.take(numCubedAttributes)
      inputDf = dataprep.prep(inputDf, "Fare_Amt", predicateDfLocation)
      dataprep.queryPredicateDf.show()
      dataprep.totalCount = inputDf.count()

      println(dataprep.generateQueryWorkload(10).deep.mkString(", "))
      inputDf.show()
      //        .withColumn("Payment_Type", when(col("Payment_Type").equalTo("crd"), "credit") otherwise(col("Payment_Type")))
      //        .withColumn("Payment_Type", when(col("Payment_Type").equalTo("csh"), "cash") otherwise(col("Payment_Type")))
      //        .withColumn("Payment_Type", when(col("Payment_Type").equalTo("unk"), "dispute") otherwise(col("Payment_Type")))
      //        .withColumn("Payment_Type", when(col("Payment_Type").equalTo("noc"), "no charge") otherwise(col("Payment_Type")))
      //        .withColumn("Payment_Type", when(col("Payment_Type").equalTo("no "), "no charge") otherwise(col("Payment_Type")))
      //        .withColumn("Payment_Type", when(col("Payment_Type").equalTo("na "), "dispute") otherwise(col("Payment_Type")))
      //        .withColumn("Payment_Type", when(col("Payment_Type").equalTo("cas"), "cash") otherwise(col("Payment_Type")))
      //        .withColumn("Payment_Type", when(col("Payment_Type").equalTo("cre"), "credit") otherwise(col("Payment_Type")))
      //        .withColumn("Payment_Type", when(col("Payment_Type").equalTo("dis"), "dispute") otherwise(col("Payment_Type")))
      //        .filter(col("Payment_Type").notEqual("1") && col("Payment_Type").notEqual("2") && col("Payment_Type").notEqual("3")
      //          && col("Payment_Type").notEqual("4") && col("Payment_Type").notEqual("5")).persist(StorageLevel.MEMORY_AND_DISK_SER)
      //      inputDf.groupBy("Trip_Pickup_DateTime").count().show()
      //      inputDf.groupBy("Trip_Dropoff_DateTime").count().show()
      //      inputDf.groupBy("Payment_Type").count().show()
      //      inputDf.groupBy("vendor_name").count().show()
      //      inputDf.groupBy("Passenger_Count").count().show()
      //      inputDf.groupBy("Rate_Code").count().show()
      //      inputDf.groupBy("store_and_forward").count().show()
      //      inputDf.groupBy("surcharge").count().show()
      //      inputDf.groupBy("mta_tax").count().show()
      //
      //      inputDf.select("Trip_Pickup_DateTime").distinct().count()
      //      inputDf.select("Trip_Dropoff_DateTime").distinct().count()
      //      inputDf.select("Payment_Type").distinct().count()
      //      inputDf.select("vendor_name").distinct().count()
      //      inputDf.select("Passenger_Count").distinct().count()
      //      inputDf.select("Rate_Code").distinct().count()
      //      inputDf.select("store_and_forward").distinct().count()
      //      inputDf.select("surcharge").distinct().count()
      //      inputDf.select("mta_tax").distinct().count()
    }
    it("Passed flights prep step") {
      var inputDf = spark.read.format("csv").option("delimiter", ",").option("header", "true").load(fligtInputLocation)
      //var inputDf = spark.read.format("csv").option("delimiter", ",").option("header", "true").load("/hdd2/data/flights.csv")
      val dataprep = new PrepFlightData
      dataprep.cubeAttributes = dataprep.cubeAttributes.take(numCubedAttributes)
      inputDf = dataprep.prep(inputDf, "AIR_TIME", predicateDfLocation)
      //inputDf.show()
      dataprep.totalCount = inputDf.count()

      dataprep.queryPredicateDf.show(100)
      println(dataprep.generateQueryWorkload(10).deep.mkString(", "))
      println(dataprep.totalPredicateCount)
    }
  }
}
