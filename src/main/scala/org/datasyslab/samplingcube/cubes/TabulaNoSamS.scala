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

import java.util.Calendar

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.datasyslab.samplingcube.algorithms.FindCombinations
import org.datasyslab.samplingcube.utils.SimplePoint

class TabulaNoSamS(sparkSession: SparkSession, inputTableName: String, totalCount: Long)
  extends Tabula(sparkSession: SparkSession, inputTableName: String, totalCount: Long) {
  /**
    * Build the pre-cube table. The pre cube table will have sample, local data measure, local sample measure
    *
    * @param cubedAttributes
    * @param sampledAttribute
    * @param qualityAttribute
    * @return
    */
  def buildCubeNoSamS(cubedAttributes: Seq[String], sampledAttribute: String, qualityAttribute: String, icebergThresholds: Seq[Double], cubeTableLocation: String, predicateDf:DataFrame
               ,payload:String): Tuple2[DataFrame, RDD[SimplePoint]] = {
    lastDryRunEndTime = 0
    lastRealRunEndTime = 0
    this.globalSample = drawGlobalSample(sampledAttribute, qualityAttribute, icebergThresholds(0))
    val cubedAttributesString = cubedAttributes.mkString(",")
//    val samplingFunctionString = generateSamplingFunction(sampledAttribute, sampleBudget)
    val samplingFunctionString = generateSamplingFunction(sampledAttribute, icebergThresholds(0))
    /** ****************************************
      * Stage 1: Dry run stage
      * ****************************************/
    var dryrunDf = dryrunWithEuclidean(cubedAttributes, sampledAttribute, qualityAttribute, icebergThresholds)
//    logger.info(cubeLogPrefix + s"dryrun find iceberg cells ${dryrunDf.count()}")
    // Persist the dryrun result on disk because Spark has a bug that will lead to infinite loop
    dryrunDf.write.mode(SaveMode.Overwrite).option("header", "true").csv(cubeTableLocation + "-" + tempTableNameDryrun)
    lastDryRunEndTime = Calendar.getInstance().getTimeInMillis
    /** ****************************************
      * Stage 2: Real run stage. Find all cuboids that can be ignored
      * ****************************************/
    dryrunDf = sparkSession.read.format("csv").option("delimiter", ",").option("header", "true").load(cubeTableLocation + "-" + tempTableNameDryrun).persist(StorageLevel.MEMORY_ONLY)
    println("dryrun find iceberg cells "+dryrunDf.count())
    // Find all cuboids that need a true GroupBy
    var realRunResultDf:DataFrame = null

    var requiredGroupbys = Math.pow(2, cubedAttributes.size)
    var filterThenGroupBy = 0

    for (i <- 1 to cubedAttributes.size) {
      // Find several cuboids
      val cuboids = FindCombinations.find(cubedAttributes, cubedAttributes.size, i)
      for (j <- 0 to (cuboids.size -1) ) {
        var icebergDf = dryrunDf
        // Check whether this cuboid has iceberg cells
        val notNullAttributes = cuboids(j).split(",")
        logger.info(cubeLogPrefix+"checking cuboid "+cuboids(j))
        notNullAttributes.foreach(f=> {
          icebergDf = icebergDf.filter(col(f).isNotNull)
        })

        var nullAttributes:Seq[String] = Seq()
        cubedAttributes.toSet.filterNot(notNullAttributes.toSet).foreach(f=>nullAttributes = nullAttributes:+f.asInstanceOf[String])
        logger.info(cubeLogPrefix+"null attributes are "+nullAttributes.mkString(","))

        nullAttributes.foreach(f=>{
          icebergDf = icebergDf.filter(col(f).isNull)
        })
        var icebergcellCount = icebergDf.count()
        if (icebergcellCount == 0) {
          // This cuboid can be skipped because it has no iceberg cells
          requiredGroupbys = requiredGroupbys - 1
          logger.info(cubeLogPrefix+"skip a cuboid")
        }
        else if (pruneFirstCondition(totalCount, icebergcellCount, predicateDf.groupBy(notNullAttributes.head, notNullAttributes.tail:_*).count().count()))
        {
          logger.info(cubeLogPrefix+"filter then build a cuboid")
          // This cuboid just has a few iceberg cells. We can first filter out useless data and then group by.
          var filteredDf = sparkSession.table(inputTableName).join(icebergDf, notNullAttributes)
          if (realRunResultDf == null) realRunResultDf = groupByCuboid(filteredDf, notNullAttributes, qualityAttribute, samplingFunctionString, icebergThresholds, nullAttributes,payload, true)
          else realRunResultDf = realRunResultDf.union(groupByCuboid(filteredDf, notNullAttributes, qualityAttribute, samplingFunctionString, icebergThresholds, nullAttributes, payload, true))
          filterThenGroupBy = filterThenGroupBy+1
        }
        else {
          logger.info(cubeLogPrefix+"build a cuboid")
          // This cuboid needs a full GroupBy
          if (realRunResultDf == null) realRunResultDf = groupByCuboid(sparkSession.table(inputTableName), notNullAttributes, qualityAttribute, samplingFunctionString, icebergThresholds, nullAttributes, payload, true)
          else realRunResultDf = realRunResultDf.union(groupByCuboid(sparkSession.table(inputTableName), notNullAttributes, qualityAttribute, samplingFunctionString, icebergThresholds, nullAttributes, payload, true))
        }
      }
    }
    println(s"required groupby: ${requiredGroupbys} filterThenGroupby $filterThenGroupBy all groupby ${Math.pow(2, cubedAttributes.size)}")

    realRunResultDf = realRunResultDf.drop(col(s"${cubeLocalMeasureName}1"))

    return (realRunResultDf.withColumn(payloadColName,lit("")), sparkSession.table(tempTableNameGLobalSample).rdd.map(f => f.getAs[SimplePoint](0)))
  }
}
