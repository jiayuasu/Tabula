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
package org.datasyslab.samplingcube.utils

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.datasyslab.samplingcube.GlobalVariables

trait CommonFunctions extends GlobalVariables with SerializableUdf {
  val stringify = udf((vs: Seq[String]) => vs match {
    case null => null
    case _ => s"""${vs.mkString(",")}"""
  })

  val logger: Logger = LogManager.getLogger(classOf[CommonFunctions])

  /**
    * Create geometry type for the column
    *
    * @param dataFrame
    * @param geomAttribute
    * @return
    */
  def createGeomType(dataFrame: DataFrame, geomAttribute: String): DataFrame = {
    return dataFrame.withColumn(geomAttribute, expr(s"CB_SimplePointFromWKT($geomAttribute)"))
  }

  /**
    * overload in case the sample is concatenated to a single long string
    *
    * @param dataFrame
    * @param queryAttributes
    * @param attributeValues
    * @param qualityAttribute
    * @param sample
    * @return
    */
  def calculateFinalLoss(dataFrame: DataFrame, queryAttributes: Seq[String], attributeValues: Seq[String], qualityAttribute: String, sample: String): Seq[Double] = {
    var sampleArray = sample.split(",").map(f=> {
      val g = f.split(" ")
      new SimplePoint(g(0).toDouble, g(1).toDouble)
    })
    if (sampleArray.length != 0) {
      return calculateFinalLoss(dataFrame, queryAttributes, attributeValues, qualityAttribute, sampleArray)
    }
    else {
      logger.warn(cubeLogPrefix+"calculateFinalLoss finds sampleArray = 0")
      return  Seq(1.0, -1, 0)
    }
  }

  /**
    * Calculate the final loss of the sample in corresponding to dataframe
    *
    * @param dataFrame
    * @param queryAttributes
    * @param attributeValues
    * @param qualityAttribute
    * @param sample
    * @return
    */
  def calculateFinalLoss(dataFrame: DataFrame, queryAttributes: Seq[String], attributeValues: Seq[String], qualityAttribute: String, sample: Array[SimplePoint]): Seq[Double] = {
    var inputTable = filterDataframe(dataFrame, queryAttributes, attributeValues, false).select(qualityAttribute)
    var rawData = inputTable.collect().map(f=>f.get(0).asInstanceOf[SimplePoint])
//    logger.info(cubeLogPrefix+s"raw data is ${rawData.mkString(",")}")
    // Calculate the sample data measure
    var samplePointType = sample.filter(p => !(p == null))//.map(f => f.toDouble)
    if (samplePointType.size == 0) {
      logger.warn(cubeLogPrefix+"calculateFinalLoss finds samplePointType size = 0")
      return Seq(1.0, -1, 0)
    }

    val loss = euclideanloss_spatial(rawData, samplePointType)
    if (loss.isNaN) return Seq(-1, -1, 0)
    logger.info(cubeLogPrefix +s"actual euclidean loss 1D is $loss")
    return Seq(loss, inputTable.count(), samplePointType.size)
  }

  /**
    * Run range query directly on a dataframe
    *
    * @param dataFrame
    * @param queryAttributes
    * @param attributeValues
    * @return
    */
  def filterDataframe(dataFrame: DataFrame, queryAttributes: Seq[String], attributeValues: Seq[String], isCubeTable: Boolean): DataFrame = {
    var inputTable = dataFrame
    // Apply query predicates
    for (i <- 0 to (queryAttributes.length - 1)) {
      // Just in case, sometimes the user wants null
      if (attributeValues(i) == null) {
        if (isCubeTable) inputTable = inputTable.filter(col(queryAttributes(i)).isNull)
      }
      else inputTable = inputTable.filter(col(queryAttributes(i)).===(attributeValues(i)))
    }
//    logger.info(cubeLogPrefix+s"filterDataframe finds: ${inputTable.count()} isCubeTable:$isCubeTable")
    return inputTable
  }

  def calculateFullTableAgg(inputDf: DataFrame, measureFunctions: Seq[String], measureColNames: Seq[String], sampleColName: String): DataFrame = {
    inputDf.createOrReplaceTempView(tempTableNameFinalLoss)
    var aggDf = inputDf.sparkSession.sql(
      s"""
         |SELECT ${generateMeasureStringForSelect(measureFunctions, measureColNames, sampleColName)}
         |FROM $tempTableNameFinalLoss
      """.stripMargin)
    aggDf
  }

  def generateMeasureStringForSelect(measureFunctions: Seq[String], measureColNames: Seq[String], qualityAttribue: String): String = {
    assert(measureFunctions.size == measureColNames.size)
    var selectClause = ""
    for (i <- 0 to (measureFunctions.size - 1)) {
      selectClause += s"${measureFunctions(i)}($qualityAttribue) AS ${measureColNames(i)},"
    }
    selectClause.dropRight(1)
  }

  /**
    * Generate greedy sampling function
    * @param sampledAttribute
    * @param threshold
    * @return
    */
  def generateSamplingFunction(sampledAttribute: String, threshold: Double): String = {
    val samplingFunction = s"CB_Sampling_Spatial(CB_MergeSpatial($sampledAttribute), $threshold)"
    return samplingFunction
  }

  def generateLossConditionString(cubeRawdataColName:String, mergeSampleString:String, icebergThreshold: Double, condition: String): String = {
    s"CB_EuclideanLoss_Spatial($cubeRawdataColName, \'$mergeSampleString\') $condition CB_Threshold(CAST(0.0 AS double), CAST($icebergThreshold AS double))"
  }

  def appendSampleMeasureColToDf(inputDf: DataFrame, sampleMeasureFunctions: Seq[String], sampleMeasureColName: Seq[String], sampleColName: String): DataFrame = {
    assert(sampleMeasureFunctions.size == sampleMeasureColName.size)
    var resultDf = inputDf
    for (i <- 0 to (sampleMeasureFunctions.size - 1)) {
      resultDf = resultDf.withColumn(sampleMeasureColName(i), expr(s"${sampleMeasureFunctions(i)}($sampleColName)"))
    }
    resultDf
  }

  def removeNanInDf(inputDf: DataFrame, measureColNames: Seq[String]): DataFrame = {
    var finalDf = inputDf
    measureColNames.foreach(f => {
      finalDf = finalDf.withColumn(f, when(col(f).isNaN, 0.0).otherwise(col(f)))
    })
    finalDf
  }

  /**
    * Given a cuboid, if [Cost(Prune) + Cost(GroupBy on PrunedData) < CostGroupAllData], then Tabula first filters the input table, then run GroupBy
    * on the remaining data. If this does not hold, Tabula will simply run a GroupBy on the entire input table
    * @param totalCount the number of rows in the input data table
    * @param icebergcells the number of iceberg cells in the cube which is derived from the input table
    * @param allcells the number of cells in the cube which is derived from the input table
    * @return
    */
  def pruneFirstCondition(totalCount:Long, icebergcells:Long, allcells:Long):Boolean = {
    var percent = icebergcells*1.0/allcells
    var pruneCost = totalCount*(icebergcells + percent*logarithmFunc(allcells, percent*totalCount))
    var groupbyCost = totalCount*logarithmFunc(allcells, totalCount)+Math.pow((totalCount*1.0/allcells), 2)
    pruneCost <= groupbyCost
  }

  def logarithmFunc(base:Long, number:Double): Double = {
    Math.log(number)*1.0/Math.log(base)
  }
}
