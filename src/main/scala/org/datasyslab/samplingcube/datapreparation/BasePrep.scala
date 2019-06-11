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
package org.datasyslab.samplingcube.datapreparation

import org.apache.spark.sql.DataFrame

trait BasePrep {
  var queryPredicateDf: DataFrame = null
  var cubeAttributes = Seq[String]()
  var totalCount = 0L
  var totalPredicateCount = 0L
  var payload:String = ""

  def generateQueryWorkload(approxNumQueries: Int): Array[Seq[Any]] = {
    return queryPredicateDf.rdd.takeSample(false, approxNumQueries, System.currentTimeMillis()).map(_.toSeq)//.sample(true, approxNumQueries * 1.0 / totalPredicateCount).collect().map(_.toSeq)
  }

  def prep(dataFrame: DataFrame, sampledAttribute: String, qualityAttribute: String, predicateDfLocation:String, dropRedundant:Boolean, rawInputPath:String): DataFrame

  def prep(dataFrame: DataFrame, sampledAttribute: String, qualityAttribute: String, predicateDfLocation:String, dropRedundant:Boolean): DataFrame
}
