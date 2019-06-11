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
package org.datasyslab.geosparksql.UDF

import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.geosparksql.expressions._

object Catalog {
  val expressions: Seq[FunctionBuilder] = Seq(
    ST_PointFromText,
    ST_PolygonFromText,
    ST_LineStringFromText,
    ST_GeomFromWKT,
    ST_GeomFromWKB,
    ST_Point,
    ST_PolygonFromEnvelope,
    ST_Contains,
    ST_Intersects,
    ST_Within,
    ST_Distance,
    ST_ConvexHull,
    ST_Envelope,
    ST_Length,
    ST_Area,
    ST_Centroid,
    ST_Transform,
    ST_Intersection,
    ST_IsValid,
    ST_PrecisionReduce,
    CB_Loss,
    CB_Threshold,
    CB_ArrayAvg,
    CB_ArrayStddev,
    CB_ArrayVariance,
    CB_ArrayKurtosis,
    CB_ArraySkewness,
    CB_EuclideanSum_1D,
    CB_EuclideanSum_Spatial,
    CB_Sampling_1D,
    CB_Sampling_Spatial,
    CB_EuclideanLoss_1D,
    CB_EuclideanLoss_Spatial,
    CB_SimplePointFromWKT,
    CB_Min_Distance_Spatial
  )

  val aggregateExpressions: Seq[UserDefinedAggregateFunction] = Seq(
    new ST_Union_Aggr,
    new ST_Envelope_Aggr,
    new CB_MergeId,
    new CB_MergeIdLimit,
    new CB_MergeSpatial,
    new CB_MergeDouble
  )
}
