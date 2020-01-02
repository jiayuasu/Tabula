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
package org.datasyslab.samplingcube.datapreparation

import org.apache.commons.codec.digest.DigestUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.storage.StorageLevel
import org.datasyslab.samplingcube.utils.CommonFunctions

class PrepTaxiData extends BasePrep with CommonFunctions {
  //  var cubeAttributes = Seq("vendor_name", "pickup_weekday", "dropoff_weekday"
  //    , "Passenger_Count", "Rate_Code", "store_and_forward", "Payment_Type", "mta_tax")
  cubeAttributes = Seq("vendor_name", "pickup_weekday", "Passenger_Count", "Payment_Type", "Rate_Code")
  payload = ""

  override def prep(dataFrame: DataFrame, sampledAttribute: String, predicateDfLocation:String, dropRedundant:Boolean, inputPath:String): DataFrame = {
    val columnNames = Seq("vendor_name", "pickup_weekday", "dropoff_weekday", "Passenger_Count", "Trip_Distance", "pickup", "Rate_Code", "store_and_forward"
      , "dropoff", "Payment_Type", "Fare_Amt", "surcharge", "mta_tax", "Tip_Amt", "Tolls_Amt", "Total_Amt")
    val predicateDfName = DigestUtils.md5Hex((inputPath+cubeAttributes.mkString("")).getBytes)
    logger.info(cubeAttributes+s"predicate df name is $predicateDfName")
    var newDf = dataFrame.toDF(columnNames: _*) //.repartition(dataFrame.rdd.getNumPartitions*3)
    newDf = createGeomType(newDf, "pickup")
    newDf = createGeomType(newDf, "dropoff")
    // Get total count
    try {
      logger.warn(cubeLogPrefix+"Try to read predicates df from disk at "+predicateDfLocation+predicateDfName)
      queryPredicateDf = dataFrame.sparkSession.read.
        format("csv").option("delimiter", ",").option("header", "true").load(predicateDfLocation+predicateDfName).persist(StorageLevel.MEMORY_AND_DISK_SER)
    }
    catch {
      case e:org.apache.spark.sql.AnalysisException => {
        logger.warn(cubeLogPrefix+"Not find that predicate df. Build predicates df from scratch at "+predicateDfName)
        queryPredicateDf = newDf.cube(cubeAttributes.map(f => col(f)): _*).count().drop(col("count")).persist(StorageLevel.MEMORY_AND_DISK_SER)
        queryPredicateDf.write.mode(SaveMode.Overwrite).option("header", "true").csv(predicateDfLocation+predicateDfName)
      }
    }
    // Get total predicate count
    totalPredicateCount = queryPredicateDf.count()
    newDf
  }

  override def prep(dataFrame: DataFrame, sampledAttribute: String, predicateDfLocation: String, dropRedundant: Boolean): DataFrame = {
    prep(dataFrame, sampledAttribute, predicateDfLocation, dropRedundant, "")
  }
}
