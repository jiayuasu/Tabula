/**
  * FILE: CubeLoader
  * Copyright (c) 2015 - 2019 Data Systems Lab at Arizona State University
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package org.datasyslab.samplingcube.cubes

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.datasyslab.samplingcube.utils.{CommonFunctions, SimplePoint}


/**
  * Construct a CubeLoader
  *
  * @param inputCubeTables (1) This cube does not have a separate sample table
  *                        The cube table has sample together inside the table
  *                        Weekday Payment Sample
  *                        1  cash  (point, point, point, ...)
  *                        2  credit  (point, point, point, ...)
  *                        (2) The cube tables incude two tables, 1 cube table and 1 sample table This happends in Tabula.
  *                        The cube table without any actual sample
  *                        Weekday Payment ID
  *                        1  cash  1
  *                        2  credit  2
  *                        The sample table which only contains the id and corresponding sample
  *                        ID sample
  *                        1  (point, point, point, ...)
  *                        2  (point, point, point, ...)
  * @param globalSampleRdd The global sample which is stored in an RDD
  */
class CubeLoader(inputCubeTables:Seq[DataFrame],globalSampleRdd: RDD[SimplePoint]) extends CommonFunctions {
  val globalSample: Array[SimplePoint] = globalSampleRdd.collect()
  val numCubeTables: Int = inputCubeTables.length

  /**
    * Search the cube table.
    *
    * @param cubedAttributes The number of attriutes that are used to construct the cube
    * @param attributeValues The exact values that are put in the predicate
    * @return a sample which is an array of points. But it is serialized to a single string.
    */
  def searchCube(cubedAttributes: Seq[String], attributeValues: Seq[String]): Tuple2[String, String] = {
    var cubeTable = filterDataframe(inputCubeTables.head, cubedAttributes, attributeValues, true)
    var sample:Array[Row] = null
    var query:String = null
    if (numCubeTables == 1) {
      sample = cubeTable.take(1)
      // Generate the search query
      query = s"""
         |SELECT sample
         |FROM SamplingCube
         |WHERE ${cubedAttributes.zip(attributeValues).map(f => f._1 + " = " + f._2).mkString(" AND ")}
    """.stripMargin
    }
    else if (numCubeTables == 2) {
      sample = cubeTable.limit(1).join(inputCubeTables(1), expr(s"$repre_id_prefix$cubeTableIdName = $cubeTableIdName")).select(cubeSampleColName).take(1)
      // Generate the search query
      query = s"""
         |SELECT sample
         |FROM Tabula_Cube_Table cube, Tabula_Sample_Table sam
         |WHERE ${cubedAttributes.zip(attributeValues).map(f => "cube." + f._1 + " = " + f._2).mkString(" AND ")} AND cube.id = sam.id
         |LIMIT 1
    """.stripMargin
    }
    else throw new IllegalArgumentException("The number of cube tables must be 1 or 2")
    // Validate the query result. cube search doesn't support rollup for now, return the first iceberg cell
    if (sample.length == 0 || sample(0).getAs[Seq[Double]](cubeSampleColName) == null) {
      logger.info(cubeLogPrefix + "cube search return the global sample")
      return (query, globalSample.deep.mkString(","))
    }
    else {
      logger.info(cubeLogPrefix + "cube search return an iceberg cell local sample")
      var icebergSample = sample(0).getAs[String](cubeSampleColName) //.toArray.map(_.toString)
      return (query, icebergSample)
    }
  }
}