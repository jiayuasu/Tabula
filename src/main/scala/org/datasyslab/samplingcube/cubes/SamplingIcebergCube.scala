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
package org.datasyslab.samplingcube.cubes

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.datasyslab.samplingcube.algorithms.FindCombinations
import org.datasyslab.samplingcube.utils.SimplePoint

/**
  *
  * @param sparkSession SparkSession
  * @param inputTableName the name of the input data table. Will be used to obtain the table object
  * @param totalCount the total number of rows in the input table
  */
class SamplingIcebergCube(sparkSession: SparkSession, inputTableName: String, totalCount: Long)
  extends BaseCube(sparkSession, inputTableName, totalCount) {

  /**
    * Build the partially materialized sampling cube using the naive construction algorithm
    *
    * @param cubedAttributes The attributes that are put in the CUBE operator. It looks like this: GROUP BY CUBE(attribute 1, attribute 2)
    * @param sampledAttribute The attribute on which we run SAMPLING function and compute the accuracy loss
    * @param icebergThreshold The threshold which we use to determine iceberg cells
    * @param payload The payload is used to simulate the other attributes in the table and such that we can do stress tests
    * @return The cube table (in a DataFrame type) and the global sample (in a RDD type)
    *         The cube table has sample together inside the table
    *         Weekday Payment Sample
    *         1  cash  (point, point, point, ...)
    *         2  credit  (point, point, point, ...)
    */
  def buildCube(cubedAttributes: Seq[String], sampledAttribute: String, icebergThreshold: Double, payload: String): Tuple2[DataFrame, RDD[SimplePoint]] = {
  this.globalSample = drawGlobalSample(sampledAttribute)
  val cubedAttributesString = cubedAttributes.mkString(",")
//  val samplingFunctionString = generateSamplingFunction(sampledAttribute, sampleBudget)
  val samplingFunctionString = generateSamplingFunction(sampledAttribute, icebergThreshold)
  logger.info(cubeLogPrefix+"generating cubing query")
  var cubeDf:DataFrame = null
  for (i <- 1 to cubedAttributes.size) {
    // Find several cuboids
    val cuboids = FindCombinations.find(cubedAttributes, cubedAttributes.size, i)
    for (j <- cuboids.indices) {
      val notNullAttributes = cuboids(j).split(",")
      logger.info(cubeLogPrefix+"building cuboid "+cuboids(j))
      var nullAttributes: Seq[String] = Seq()
      cubedAttributes.toSet.filterNot(notNullAttributes.toSet).foreach(f => nullAttributes = nullAttributes :+ f.asInstanceOf[String])
      logger.info(cubeLogPrefix + "null attributes are " + nullAttributes.mkString(","))
      if (cubeDf == null) cubeDf = groupByCuboid(sparkSession.table(inputTableName), notNullAttributes, sampledAttribute, samplingFunctionString, icebergThreshold, nullAttributes, payload, true)
      else cubeDf = cubeDf.union(groupByCuboid(sparkSession.table(inputTableName), notNullAttributes, sampledAttribute, samplingFunctionString, icebergThreshold, nullAttributes, payload, true))
    }
  }
  return (cubeDf.withColumn(payloadColName,lit(""))//.repartition(sparkSession.table(inputTableName).rdd.getNumPartitions).withColumn(payloadColName,lit(s"${Seq.fill(sampleBudget)(payload).mkString("")}"))
    , sparkSession.table(tempTableNameGLobalSample).rdd.map(f => f.getAs[SimplePoint](0)))
}
}