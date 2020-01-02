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
package org.datasyslab.samplingcube.relatedwork

import java.util.Calendar

import org.apache.spark.sql.SparkSession
import org.datasyslab.samplingcube.utils.{CommonFunctions, SimplePoint}

/**
  * Construct a SampleLater class
  * @param sparkSession
  * @param inputTableName the input data table
  * @param sampleBudget the number of records in the sample
  */
class SampleLater(var sparkSession: SparkSession, var inputTableName: String, var sampleBudget: Int) extends CommonFunctions {

  /**
    * Search on the entire data then sample the query result
    * @param queryAttributes The number of attriutes that are used to construct the cube
    * @param attributeValues The exact values that are put in the predicate
    * @param sampledAttribute The attribute on which we run SAMPLING function and compute the accuracy loss
    * @param icebergThreshold The threshold which we use to determine iceberg cells
    * @return
    */
  def search(queryAttributes: Seq[String], attributeValues: Seq[String], sampledAttribute: String, icebergThreshold: Double): Array[SimplePoint] = {
    var inputTable = filterDataframe(sparkSession.table(inputTableName), queryAttributes, attributeValues, false)
    // Draw a sample on the query result
    val queryResultCount = inputTable.count()
    lastQueryFinishTime = Calendar.getInstance().getTimeInMillis
    if (queryResultCount == 0) return Array[SimplePoint]()
    logger.info(cubeLogPrefix + "sample percent = " + calSampleSize(queryResultCount, epsilon, delta) * 1.0 / queryResultCount + " query actual result count = "+queryResultCount)
    inputTable = if((calSampleSize(queryResultCount, epsilon, delta)*1.0 / queryResultCount) < 1.0)
    {inputTable.sample(true, calSampleSize(queryResultCount, epsilon, delta)*1.0 / queryResultCount)}
    else {inputTable}
    //.limit(sampleBudget)
    return if(queryResultCount>1) {sampling_spatial_EucOpt(inputTable.select(sampledAttribute).collect().map(_.getAs[SimplePoint](0)), icebergThreshold, false)}
    else {inputTable.select(sampledAttribute).collect().map(_.getAs[SimplePoint](0))}
  }

  /**
    * Clean the cached dataframe
    */
  def clean(): Unit = {
    sparkSession.table(inputTableName).unpersist()
  }
}
