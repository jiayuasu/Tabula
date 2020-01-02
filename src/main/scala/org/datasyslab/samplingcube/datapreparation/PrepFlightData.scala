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
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.storage.StorageLevel
import org.datasyslab.samplingcube.utils.CommonFunctions

class PrepFlightData extends BasePrep with CommonFunctions {
  cubeAttributes = Seq("DAY_OF_WEEK", "AIRLINE")
  payload = ""
  //cubeAttributes = Seq("DAY_OF_WEEK", "AIRLINE", "FLIGHT_NUMBER", "ORIGIN_AIRPORT", "DESTINATION_AIRPORT")
  override def prep(dataFrame: DataFrame, sampledAttribute: String, predicateDfLocation:String, inputPath:String): DataFrame = {
    var newDf = dataFrame //.persist(StorageLevel.MEMORY_AND_DISK_SER)
    val predicateDfName = DigestUtils.md5Hex((inputPath+cubeAttributes.mkString("")).getBytes)
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
    }    // Get total predicate count
    totalPredicateCount = queryPredicateDf.count()
    newDf
  }

  override def prep(dataFrame: DataFrame, sampledAttribute: String, predicateDfLocation: String): DataFrame = {
    prep(dataFrame, sampledAttribute, predicateDfLocation, "")
  }
}
