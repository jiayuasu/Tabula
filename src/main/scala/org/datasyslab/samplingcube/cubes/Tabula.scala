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

import java.util
import java.util.Calendar

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.datasyslab.samplingcube.algorithms.{FindCombinations, MinimumDominatingSet}

import scala.collection.JavaConverters._

class Tabula(sparkSession: SparkSession, inputTableName: String, totalCount: Long)
  extends BaseCube(sparkSession: SparkSession, inputTableName: String, totalCount: Long) {
  var lastDryRunEndTime:Long = 0
  var lastRealRunEndTime:Long = 0
  /**
    * Build the pre-cube table. The pre cube table will have sample, local data measure, local sample measure
    *
    * @param cubedAttributes
    * @param sampledAttribute
    * @param qualityAttribute
    * @return
    */
  def buildCube(cubedAttributes: Seq[String], sampledAttribute: String, qualityAttribute: String, icebergThresholds: Seq[Double], cubeTableLocation: String, predicateDf:DataFrame
               ,payload:String): Tuple2[DataFrame, DataFrame] = {
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

    // Append unique id to each sample, repartition to reduce the crazy number of partitions caused by union
    realRunResultDf = realRunResultDf.withColumn(cubeTableIdName, monotonically_increasing_id())//.persist(StorageLevel.MEMORY_AND_DISK_SER)
    realRunResultDf.write.mode(SaveMode.Overwrite).option("header", "true").csv(cubeTableLocation + "-" + cubeTableBeforeSamS)
    realRunResultDf = sparkSession.read.format("csv").option("delimiter", ",").option("header", "true").load(cubeTableLocation + "-" + cubeTableBeforeSamS)//.persist(StorageLevel.MEMORY_ONLY)
    lastRealRunEndTime = Calendar.getInstance().getTimeInMillis
    /** ****************************************
      * Stage 3: Sample selection stage
      * ****************************************/
    var selectedEdges = sampleSelection(realRunResultDf, icebergThresholds, cubedAttributes)

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

    logger.info(cubeLogPrefix + "final sample table count " + sampleTable.count())

    return (cubeTable, sampleTable)//.withColumn(cubeSampleColName, stringify(col(cubeSampleColName))))
  }


  private def sampleSelection(cubeTable: DataFrame, icebergThresholds: Seq[Double], cubeAttributes:Seq[String]): util.List[Row] = {
    var cubeTable1 = cubeTable
    // Drop all useless column for the self join, reduce the partition number to a reasonable no.
    cubeTable1 = cubeTable1.select(cubeTableIdName, cubeRawdataColName, cubeSampleColName).repartition(Math.pow(cubeAttributes.size, 2).toInt)
    // Create a copy of the cube table. This is silly but I have to do this because Spark 2.3 has a bug which cannot use table alias in self join
    var newColNames = cubeTable1.columns.map(f => f + "2")
    var cubeTable2 = cubeTable1.toDF(newColNames: _*).drop(cubeRawdataColName+"2")
    cubeTable1 = cubeTable1.drop(cubeSampleColName)
    // Control the number of edges.
    var final_thresholds: Seq[Double] = (globalSampleLoss zip icebergThresholds).map { case (gLoss, iThreshold) => threshold(gLoss, Seq(iThreshold, samGraphSizeControl)) }
    /**
      * Use equality join to build the sample selection graph for euclidean metric
      */
    var sampleGraph = equalityJoin(cubeTable1, cubeTable2, final_thresholds)
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

  /**
    * Search the cube
    *
    * @param cubedAttributes
    * @param attributeValues
    * @return
    */
  def searchCube(inputCubeTable: DataFrame, cubedAttributes: Seq[String], attributeValues: Seq[String], sampleTable: DataFrame): String = {
    var cubeTable = filterDataframe(inputCubeTable, cubedAttributes, attributeValues, true)
    //cubeTable = cubeTable.select(cubeSampleColName)
    val sample = cubeTable.limit(1).join(sampleTable, expr(s"$repre_id_prefix$cubeTableIdName = $cubeTableIdName")).select(cubeSampleColName).take(1)

    // Validate the query result. cube search doesn't support rollup for now, return the first iceberg cell
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

  def dryrunWithEuclidean(cubedAttributes: Seq[String], sampledAttribute: String, qualityAttribute: String, icebergThresholds: Seq[Double]): DataFrame = {
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
      """.stripMargin).filter(s"${cubeLocalMeasureName(0)} > ${icebergThresholds(0)}")
    dryrunDf
  }

  def equalityJoin(cubeTable1:DataFrame, cubeTable2:DataFrame, final_thresholds:Seq[Double]):DataFrame = {
    cubeTable1.as("df1").join(cubeTable2.as("df2"), expr(s"CB_EuclideanLoss_Spatial(df1.${cubeRawdataColName}, df2.${cubeSampleColName}2) <= ${final_thresholds(0)}"))
  }
}
