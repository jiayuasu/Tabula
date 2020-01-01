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
package org.datasyslab.samplingcube.relatedwork

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.datasyslab.samplingcube.utils.{CommonFunctions, SerializableUdf, SimplePoint}

/**
  * Construct a SampleFirst class
  * @param sparkSession
  * @param inputTableName the input data table
  * @param sampleBudget the number of records in the sample
  * @param totalCount the total number of rows in the input data table
  */
class SampleFirst(var sparkSession: SparkSession, var inputTableName: String, var sampleBudget: Int, var totalCount: Long) extends SerializableUdf with CommonFunctions {
  var sampleDf: DataFrame = null

  /**
    * Build a global sample
    */
  def build(): DataFrame = {
    // Draw sample
    sampleDf = sparkSession.table(inputTableName).sample(true, sampleBudget * 1.0 / totalCount).limit(sampleBudget).persist(StorageLevel.MEMORY_AND_DISK_SER)
    sampleDf.createOrReplaceTempView(tempTableNameGLobalSample)
    return sampleDf
  }

  /**
    * Search on the sample
    *
    * @param queryAttributes The number of attriutes that are used to construct the cube
    * @param attributeValues The exact values that are put in the predicate
    * @param sampledAttribute The attribute on which we run SAMPLING function and compute the accuracy loss
    * @return
    */
  def search(queryAttributes: Seq[String], attributeValues: Seq[String], sampledAttribute: String): Array[SimplePoint] = {
    var localSampleDf = filterDataframe(sampleDf, queryAttributes, attributeValues, false)
    val queryResultDf = localSampleDf
    if (queryResultDf.count() > 0) {
      logger.info(cubeLogPrefix + "search query finds result")
      return localSampleDf.select(sampledAttribute).collect().map(_.getAs[SimplePoint](0))
    }
    else {
      logger.info(cubeLogPrefix + "search query doesn't find result")
      return Array[SimplePoint]()
    }
  }

  /**
    * Clean the cached dataframe
    */
  def clean(): Unit = {
    sampleDf.unpersist()
  }
}
