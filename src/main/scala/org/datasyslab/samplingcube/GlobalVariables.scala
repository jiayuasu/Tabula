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
package org.datasyslab.samplingcube

trait GlobalVariables {
  val InvalidPercentileAccuracyNumber: Int = -1
  val InvalidMeasureValue: Double = -999999.9999
  val InvalidLossValue: Double = -999999.9999
  val InvalidTime: Long = -1
  val cubeLogPrefix = "[Tabula] "
  val cubeTableIdName = "id"
  val cubeSampleColName = "sample"
  val cubeRawdataColName = "rawdata"
  val repre_id_prefix = "rep"
  val finalRepColName = "finalrepid"
  // SampleFirst method
  val queryResultTableName = "queryresulttable"
  val samGraphSizeControl = 1 //0.4
//  val filterFirstThreshold = 0.1
  val payloadColName = "payload"

  //  val icebergsampleName = "icebergsampletable"

  /** *****************************************
    * Catalog that manage all temp table names. No duplicates are allowed.
    * *****************************************/
  val globalSampleName = "globalsample"
  val samGraphTableName = "samgraph"
  val tempTableNameGLobalSample = "tempfiledryrun"
  val tempTableNameDryrun = "tempfileglobalsample"
  // temp table for sampling iceberg cube with sample selection later
  val tempTableNameGroupByForSample = "tempgroupby"
  val cubeTableBeforeSamS = "beforesams"
  // temp table for sampling iceberg cube with sample selection first
  val tempTableNameFinalLoss = "tempfilefinalloss"
  val compositeTableTemp = "compositetable"
  /** *****************************************
    * Values that control the iceberg conditions and measure function. They are all seq types at the same size. Each seq allows no duplicates.
    * *****************************************/
  val cubeLocalMeasureName = Seq("l_m1")
  val cubeLocalSampleMeasureName = Seq("ls_m1")

  val InvalidMeasureValues = Seq(InvalidMeasureValue)
  val InvalidLossValues = Seq(InvalidLossValue)

  var globalSampleLoss: Seq[Double] = InvalidLossValues
  var globalSampleMeasure: Seq[Double] = InvalidMeasureValues
  var lastQueryFinishTime = InvalidTime

  var epsilon = 0.00
  var delta = 0.00

  /** *****************************************
    * Values that normalize the euclidean distance
    * *****************************************/
  /**
    * Taxi pickup = 0.680074
    */
  var maximumDistance = 0.680074
}