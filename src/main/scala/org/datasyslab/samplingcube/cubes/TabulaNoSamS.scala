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

/**
  *
  * @param sparkSession SparkSession
  * @param inputTableName the name of the input data table. Will be used to obtain the table object
  * @param totalCount the total number of rows in the input table
  */
class TabulaNoSamS(sparkSession: SparkSession, inputTableName: String, totalCount: Long)
  extends Tabula(sparkSession: SparkSession, inputTableName: String, totalCount: Long) {

  /**
    * Build the partially materialized sampling cube using the dry-run algorithm but no sample selection technique
    *
    * @param cubedAttributes The attributes that are put in the CUBE operator. It looks like this: GROUP BY CUBE(attribute 1, attribute 2)
    * @param sampledAttribute The attribute on which we run SAMPLING function and compute the accuracy loss
    * @param icebergThreshold The threshold which we use to determine iceberg cells
    * @param cubeTableLocation The path on which a temporary table will be stored. The dry run stage will produce a temporary table. Theoretically,
    *                          this is just an in-memory intermediate data. However, due to a bug in SparkSQL, this temp table must be persisted on
    *                          disk and loaded back to Spark. Otherwise, the queries that depend on this table will take forever.
    * @param predicateDf The table which contains all cells in the cube which is derived from the input table. This should be done in the data preparation
    *                    phase. SparkSQL can easily produce this table using its original CUBE operator with a COUNT aggregate.
    * @param payload The payload is used to simulate the other attributes in the table and such that we can do stress tests
    * @return The cube tables (in a DataFrame type) and the global sample (in a RDD type)
    *         The cube tables incude two tables, 1 cube table and 1 sample table This happends in Tabula.
    *         The cube table without any actual sample
    *         Weekday Payment ID
    *         1  cash  1
    *         2  credit  2
    *         The sample table which only contains the id and corresponding sample
    *         ID sample
    *         1  (point, point, point, ...)
    *         2  (point, point, point, ...)
    */
  def buildCubeNoSamS(cubedAttributes: Seq[String], sampledAttribute: String, icebergThreshold: Double, cubeTableLocation: String, predicateDf:DataFrame
               ,payload:String): Tuple2[DataFrame, RDD[SimplePoint]] = {
    lastDryRunEndTime = 0
    lastRealRunEndTime = 0
    this.globalSample = drawGlobalSample(sampledAttribute)
    val cubedAttributesString = cubedAttributes.mkString(",")
//    val samplingFunctionString = generateSamplingFunction(sampledAttribute, sampleBudget)
    val samplingFunctionString = generateSamplingFunction(sampledAttribute, icebergThreshold)
    /** ****************************************
      * Stage 1: Dry run stage
      * ****************************************/
    var dryrunDf = dryrunWithEuclidean(cubedAttributes, sampledAttribute, icebergThreshold)
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
      for (j <- cuboids.indices ) {
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
          if (realRunResultDf == null) realRunResultDf = groupByCuboid(filteredDf, notNullAttributes, sampledAttribute, samplingFunctionString, icebergThreshold, nullAttributes,payload, true)
          else realRunResultDf = realRunResultDf.union(groupByCuboid(filteredDf, notNullAttributes, sampledAttribute, samplingFunctionString, icebergThreshold, nullAttributes, payload, true))
          filterThenGroupBy = filterThenGroupBy+1
        }
        else {
          logger.info(cubeLogPrefix+"build a cuboid")
          // This cuboid needs a full GroupBy
          if (realRunResultDf == null) realRunResultDf = groupByCuboid(sparkSession.table(inputTableName), notNullAttributes, sampledAttribute, samplingFunctionString, icebergThreshold, nullAttributes, payload, true)
          else realRunResultDf = realRunResultDf.union(groupByCuboid(sparkSession.table(inputTableName), notNullAttributes, sampledAttribute, samplingFunctionString, icebergThreshold, nullAttributes, payload, true))
        }
      }
    }
    println(s"required groupby: ${requiredGroupbys} filterThenGroupby $filterThenGroupBy all groupby ${Math.pow(2, cubedAttributes.size)}")

    realRunResultDf = realRunResultDf.drop(col(s"${cubeLocalMeasureName}1"))

    return (realRunResultDf.withColumn(payloadColName,lit("")), sparkSession.table(tempTableNameGLobalSample).rdd.map(f => f.getAs[SimplePoint](0)))
  }
}
