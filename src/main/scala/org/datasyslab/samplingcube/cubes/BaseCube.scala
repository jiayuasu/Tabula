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

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.datasyslab.samplingcube.utils.{CommonFunctions, SimplePoint}

class BaseCube(var sparkSession: SparkSession, var inputTableName: String, var totalCount: Long) extends CommonFunctions {
  val stringify = udf((vs: Seq[String]) => vs match {
    case null => null
    case _ => s"""${vs.mkString(",")}"""
  })
  var globalSample: Array[SimplePoint] = null

  /**
    * Search the cube
    *
    * @param cubedAttributes
    * @param attributeValues
    * @return
    */
  def searchCube(inputCubeTable: DataFrame, cubedAttributes: Seq[String], attributeValues: Seq[String]): String = {
    var cubeTable = filterDataframe(inputCubeTable, cubedAttributes, attributeValues, true)
    // Validate the query result. cube search doesn't support rollup for now, return the first iceberg cell
    val sample = cubeTable.take(1)
    if (sample.size == 0 || sample(0).getAs[Seq[Double]](cubeSampleColName) == null) {
      logger.info(cubeLogPrefix + "cube search return the global sample")
      return globalSample.deep.mkString(",")
    }
    else {
      logger.info(cubeLogPrefix + "cube search return an iceberg cell local sample")
      var icebergSample = sample(0).getAs[String](cubeSampleColName) //.toArray.map(_.toString)
      return icebergSample
    }
  }

  /**
    * Draw a sample on the global data and calculate global sample loss, global data measure
    *
    * @param sampledAttribute
    * @param qualityAttribute
    * @return
    */
  def drawGlobalSample(sampledAttribute: String, qualityAttribute: String, threshold: Double): Array[SimplePoint] = {
    // Draw sample
    val error  = 0.05
    val confidence = 0.01
    val sampleSize = calSampleSize(totalCount, error, confidence)
    val sampleData = sparkSession.table(inputTableName).rdd.takeSample(false, sampleSize, System.currentTimeMillis())
    val sampleRDD = sparkSession.sparkContext.parallelize(sampleData)
    var sampleDf = sparkSession.createDataFrame(sampleRDD, sparkSession.table(inputTableName).schema)
    logger.warn(cubeLogPrefix + "global sample size = " + sampleSize + s" error = $error confidence = $confidence")
    //    var sampleDf = sparkSession.table(inputTableName).sample(true, sampleBudget * 1D / totalCount).limit(sampleBudget)
    sampleDf.createOrReplaceTempView(tempTableNameGLobalSample)
    sampleDf = sampleDf.select(sampledAttribute)
    globalSample = sampleDf.collect().map(_.getAs[SimplePoint](0))
    return globalSample
  }

  def groupByCuboid(input:DataFrame, groupByAttributes:Seq[String], qualityAttribute: String, samplingFunctionString:String, icebergThresholds: Seq[Double], nullAttributes:Seq[String]
                    ,payload:String, removeIceberg:Boolean):DataFrame = {
    val sparkSession = input.sparkSession
    input.createOrReplaceTempView(tempTableNameGroupByForSample)
    var groupByAttributeString = groupByAttributes.mkString(",")
    var sqlString = ""
    if (removeIceberg) {
      sqlString = s"""
                     |SELECT $groupByAttributeString, $samplingFunctionString AS $cubeSampleColName, CB_MergeSpatial($qualityAttribute) AS $cubeRawdataColName
                     |FROM $tempTableNameGroupByForSample
                     |GROUP BY $groupByAttributeString
                     |HAVING ${generateLossConditionString(cubeRawdataColName, globalSample.mkString(","), icebergThresholds(0), ">")}
      """.stripMargin
    }
    else {//, '${Seq.fill(sampleBudget)(payload).mkString("")}' AS $payloadColName
      sqlString = s"""
                     |SELECT $groupByAttributeString, $samplingFunctionString AS $cubeSampleColName
                     |FROM $tempTableNameGroupByForSample
                     |GROUP BY $groupByAttributeString
      """.stripMargin
    }
    var result = sparkSession.sql(sqlString)//appendSampleMeasureColToDf(sparkSession.sql(sqlString), sampleMeasureFunction, cubeLocalSampleMeasureName, cubeSampleColName)
      .withColumn(cubeSampleColName, stringify(col(cubeSampleColName)))
    // Fill in null for nullAttributes
    nullAttributes.foreach(f=>{
      result = result.withColumn(f, lit(null).cast(StringType))
    })
    // Reorder column position
    var columns = result.columns
    scala.util.Sorting.quickSort(columns)
    result = result.select(columns.head, columns.tail:_*)
    result.repartition(groupByAttributes.size)
  }
}
