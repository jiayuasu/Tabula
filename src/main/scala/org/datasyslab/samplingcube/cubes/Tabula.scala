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
package org.datasyslab.samplingcube.cubes

import java.util
import java.util.Calendar

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.datasyslab.samplingcube.algorithms.{FindCombinations, MinimumDominatingSet}
import org.datasyslab.samplingcube.utils.SimplePoint

import scala.collection.JavaConverters._

/**
  *
  * @param sparkSession SparkSession
  * @param inputTableName the name of the input data table. Will be used to obtain the table object
  * @param totalCount the total number of rows in the input table
  */
class Tabula(sparkSession: SparkSession, inputTableName: String, totalCount: Long)
  extends BaseCube(sparkSession: SparkSession, inputTableName: String, totalCount: Long) {
  var lastDryRunEndTime:Long = 0
  var lastRealRunEndTime:Long = 0

  /**
    * Build the partially materialized sampling cube using the dry-run algorithm and sample selection technique
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
  def buildCube(cubedAttributes: Seq[String], sampledAttribute: String, icebergThreshold: Double, cubeTableLocation: String, predicateDf:DataFrame
               ,payload:String): Tuple3[DataFrame, DataFrame, RDD[SimplePoint]] = {
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
    // Persist the dryrun result on disk because Spark has a bug that will lead to infinite loop
    dryrunDf.write.mode(SaveMode.Overwrite).option("header", "true").csv(cubeTableLocation + "-" + tempTableNameDryrun)
    lastDryRunEndTime = Calendar.getInstance().getTimeInMillis
    /** ****************************************
      * Stage 2: Real run stage. Find all cuboids that can be ignored
      * ****************************************/
    dryrunDf = sparkSession.read.format("csv").option("delimiter", ",").option("header", "true").load(cubeTableLocation + "-" + tempTableNameDryrun).persist(StorageLevel.MEMORY_ONLY)
    logger.info(cubeLogPrefix + "[Dry-run stage] Found iceberg cells "+dryrunDf.count())
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
        notNullAttributes.foreach(f=> {
          icebergDf = icebergDf.filter(col(f).isNotNull)
        })

        var nullAttributes:Seq[String] = Seq()
        cubedAttributes.toSet.filterNot(notNullAttributes.toSet).foreach(f=>nullAttributes = nullAttributes:+f.asInstanceOf[String])
        logger.info(cubeLogPrefix+s"[Real-run stage] Now checking where cuboid ${cuboids(j)} (${nullAttributes.mkString(",")} = NULL) has iceberg cells...")

        nullAttributes.foreach(f=>{
          icebergDf = icebergDf.filter(col(f).isNull)
        })
        var icebergcellCount = icebergDf.count()
        if (icebergcellCount == 0) {
          // This cuboid can be skipped because it has no iceberg cells
          requiredGroupbys = requiredGroupbys - 1
          logger.info(cubeLogPrefix+s"[Real-run stage] Skip cuboid ${cuboids(j)} because it has no iceberg cells")
        }
        else if (pruneFirstCondition(totalCount, icebergcellCount, predicateDf.groupBy(notNullAttributes.head, notNullAttributes.tail:_*).count().count()))
        {
          logger.info(cubeLogPrefix+s"[Real-run stage] Filter the input table then build cuboid ${cuboids(j)}")
          // This cuboid just has a few iceberg cells. We can first filter out useless data and then group by.
          var filteredDf = sparkSession.table(inputTableName).join(icebergDf, notNullAttributes)
          if (realRunResultDf == null) realRunResultDf = groupByCuboid(filteredDf, notNullAttributes, sampledAttribute, samplingFunctionString, icebergThreshold, nullAttributes,payload, true)
          else realRunResultDf = realRunResultDf.union(groupByCuboid(filteredDf, notNullAttributes, sampledAttribute, samplingFunctionString, icebergThreshold, nullAttributes, payload, true))
          filterThenGroupBy = filterThenGroupBy+1
        }
        else {
          logger.info(cubeLogPrefix+s"[Real-run stage] Build cuboid ${cuboids(j)} using a complete GroupBy")
          // This cuboid needs a full GroupBy
          if (realRunResultDf == null) realRunResultDf = groupByCuboid(sparkSession.table(inputTableName), notNullAttributes, sampledAttribute, samplingFunctionString, icebergThreshold, nullAttributes, payload, true)
          else realRunResultDf = realRunResultDf.union(groupByCuboid(sparkSession.table(inputTableName), notNullAttributes, sampledAttribute, samplingFunctionString, icebergThreshold, nullAttributes, payload, true))
        }
      }
    }
    logger.info(cubeLogPrefix+s"[Real-run stage] Found iceberg cuboids: ${requiredGroupbys.toInt} (FilterThenGroupby $filterThenGroupBy); All cuboids in the complete cube: ${Math.pow(2, cubedAttributes.size).toInt}")

    // Append unique id to each sample, repartition to reduce the crazy number of partitions caused by union
    realRunResultDf = realRunResultDf.withColumn(cubeTableIdName, monotonically_increasing_id())//.persist(StorageLevel.MEMORY_AND_DISK_SER)
    realRunResultDf.write.mode(SaveMode.Overwrite).option("header", "true").csv(cubeTableLocation + "-" + cubeTableBeforeSamS)
    realRunResultDf = sparkSession.read.format("csv").option("delimiter", ",").option("header", "true").load(cubeTableLocation + "-" + cubeTableBeforeSamS)//.persist(StorageLevel.MEMORY_ONLY)
    lastRealRunEndTime = Calendar.getInstance().getTimeInMillis
    /** ****************************************
      * Stage 3: Sample selection stage
      * ****************************************/
    var selectedEdges = sampleSelection(realRunResultDf, icebergThreshold, cubedAttributes)

    val selectedEdgesSchema = new StructType()
      .add(cubeTableIdName, LongType)
      .add(repre_id_prefix + cubeTableIdName, LongType)

    // Find the representative cell set
    val selectedEdgesDf = sparkSession.createDataFrame(selectedEdges, selectedEdgesSchema)

    // Find the cube table
    var cubeTable = realRunResultDf.as("df1").join(selectedEdgesDf.as("df2"), expr(s"df1.$cubeTableIdName == df2.$cubeTableIdName"))
      .select(repre_id_prefix + cubeTableIdName, cubedAttributes:_*)
    // Find the sample table
    var sampleTable = realRunResultDf.as("df1").join(selectedEdgesDf.select(repre_id_prefix + cubeTableIdName).distinct().as("df2"), expr(s"df1.$cubeTableIdName == df2.${repre_id_prefix + cubeTableIdName}"))
      .withColumn(payloadColName,lit("")).select(s"df1.$cubeTableIdName", cubeSampleColName, payloadColName)

    logger.info(cubeLogPrefix + s"[Table normalization] Normalized cube tables: cube table (count: ${cubeTable.count()}); sample table (count: ${sampleTable.count()})")

    return (cubeTable, sampleTable, sparkSession.table(tempTableNameGLobalSample).rdd.map(f => f.getAs[SimplePoint](0)))//.withColumn(cubeSampleColName, stringify(col(cubeSampleColName))))
  }


  private def sampleSelection(cubeTable: DataFrame, icebergThreshold: Double, cubeAttributes:Seq[String]): util.List[Row] = {
    var cubeTable1 = cubeTable
    // Drop all useless column for the self join, reduce the partition number to a reasonable no.
    cubeTable1 = cubeTable1.select(cubeTableIdName, cubeRawdataColName, cubeSampleColName).repartition(Math.pow(cubeAttributes.size, 2).toInt)
    // Create a copy of the cube table. This is silly but I have to do this because Spark 2.3 has a bug which cannot use table alias in self join
    var newColNames = cubeTable1.columns.map(f => f + "2")
    var cubeTable2 = cubeTable1.toDF(newColNames: _*).drop(cubeRawdataColName+"2")
    cubeTable1 = cubeTable1.drop(cubeSampleColName)
    // Control the number of edges.
    /**
      * Use equality join to build the sample selection graph for euclidean metric
      */
    var sampleGraph = equalityJoin(cubeTable1, cubeTable2, icebergThreshold)
//    logger.warn(cubeLogPrefix+"samgraph edges are "+sampleGraph.count())
    sampleGraph.createOrReplaceTempView(samGraphTableName)
    sampleGraph = sparkSession.sql(
      s"""
         |SELECT ${cubeTableIdName}2, CB_MergeId(${cubeTableIdName}) AS ${cubeTableIdName}
         |FROM $samGraphTableName
         |GROUP BY ${cubeTableIdName}2
      """.stripMargin)
    val edges = MinimumDominatingSet.GreedyAlgorithm(sampleGraph.collect()).toList.asJava
    return edges
  }

  def dryrunWithEuclidean(cubedAttributes: Seq[String], sampledAttribute: String, icebergThreshold: Double): DataFrame = {
//    this.globalSample = drawGlobalSample(sampledAttribute, qualityAttribute, icebergThresholds(0))
    var cubedAttributesString = cubedAttributes.mkString(",")
    var globalSampleString = globalSample.mkString(",")
    var topCuboid = sparkSession.sql(
      s"""
         |SELECT $cubedAttributesString, sum(CB_Min_Distance_Spatial($sampledAttribute, '$globalSampleString')) AS ${cubeLocalMeasureName(0)+"sum"}, count(*) AS ${cubeLocalMeasureName(0)+"count"}
         |FROM $inputTableName
         |GROUP BY $cubedAttributesString
      """.stripMargin)
    topCuboid.createOrReplaceTempView(tempTableNameDryrun+"topcuboid")
    var tempDf = sparkSession.sql(
      s"""
         |SELECT $cubedAttributesString, sum(${cubeLocalMeasureName(0)+"sum"}) AS ${cubeLocalMeasureName(0)+"sum"}, sum(${cubeLocalMeasureName(0)+"count"}) AS ${cubeLocalMeasureName(0)+"count"}
         |FROM ${tempTableNameDryrun+"topcuboid"}
         |GROUP BY CUBE($cubedAttributesString)
      """.stripMargin)
    tempDf.createOrReplaceTempView(tempTableNameDryrun+"intermediatetable")
    var dryrunDf = sparkSession.sql(
      s"""
        |SELECT $cubedAttributesString, (${cubeLocalMeasureName(0)+"sum"}/${cubeLocalMeasureName(0)+"count"}) AS ${cubeLocalMeasureName(0)}
        |FROM ${tempTableNameDryrun+"intermediatetable"}
      """.stripMargin).filter(s"${cubeLocalMeasureName(0)} > ${icebergThreshold}")
    dryrunDf
  }

  def equalityJoin(cubeTable1:DataFrame, cubeTable2:DataFrame, final_threshold: Double):DataFrame = {
    cubeTable1.as("df1").join(cubeTable2.as("df2"), expr(s"CB_EuclideanLoss_Spatial(df1.${cubeRawdataColName}, df2.${cubeSampleColName}2) <= ${final_threshold}"))
  }
}