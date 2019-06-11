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

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.datasyslab.samplingcube.algorithms.FindCombinations

class SamplingIcebergCube(sparkSession: SparkSession, inputTableName: String, totalCount: Long)
  extends BaseCube(sparkSession, inputTableName, totalCount) {

  /**
    * Build the pre-cube table. The pre cube table will have sample, local data measure, local sample measure
    *
    * @param cubedAttributes
    * @param sampledAttribute
    * @param qualityAttribute
    * @return
    */
def buildCube(cubedAttributes: Seq[String], sampledAttribute: String, qualityAttribute: String, icebergThresholds: Seq[Double], payload: String): DataFrame = {
  this.globalSample = drawGlobalSample(sampledAttribute, qualityAttribute, icebergThresholds(0))
  val cubedAttributesString = cubedAttributes.mkString(",")
//  val samplingFunctionString = generateSamplingFunction(sampledAttribute, sampleBudget)
  val samplingFunctionString = generateSamplingFunction(sampledAttribute, icebergThresholds(0))
  logger.info(cubeLogPrefix+"generating cubing query")
  var cubeDf:DataFrame = null
  for (i <- 1 to cubedAttributes.size) {
    // Find several cuboids
    val cuboids = FindCombinations.find(cubedAttributes, cubedAttributes.size, i)
    for (j <- 0 to (cuboids.size - 1)) {
      val notNullAttributes = cuboids(j).split(",")
      logger.info(cubeLogPrefix+"building cuboid "+cuboids(j))
      var nullAttributes: Seq[String] = Seq()
      cubedAttributes.toSet.filterNot(notNullAttributes.toSet).foreach(f => nullAttributes = nullAttributes :+ f.asInstanceOf[String])
      logger.info(cubeLogPrefix + "null attributes are " + nullAttributes.mkString(","))
      if (cubeDf == null) cubeDf = groupByCuboid(sparkSession.table(inputTableName), notNullAttributes, qualityAttribute, samplingFunctionString, icebergThresholds, nullAttributes, payload, true)
      else cubeDf = cubeDf.union(groupByCuboid(sparkSession.table(inputTableName), notNullAttributes, qualityAttribute, samplingFunctionString, icebergThresholds, nullAttributes, payload, true))
    }
  }
  return cubeDf.withColumn(payloadColName,lit(""))//.repartition(sparkSession.table(inputTableName).rdd.getNumPartitions).withColumn(payloadColName,lit(s"${Seq.fill(sampleBudget)(payload).mkString("")}"))
}
}