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
package org.apache.spark.sql.geosparksql.expressions

import com.vividsolutions.jts.geom.{Coordinate, Geometry, GeometryFactory, Polygon}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.aggregate.DeclarativeAggregate
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, If, IsNull, LessThan, Literal, Or}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.geosparksql.UDT.{GeometryUDT, SimplePointUDT}
import org.apache.spark.sql.types.{StructField, _}
import org.datasyslab.samplingcube.GlobalVariables
import org.datasyslab.samplingcube.utils.SimplePoint

/**
  * Return the polygon union of all Polygon in the given column
  */

class ST_Union_Aggr extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = StructType(StructField("Union", new GeometryUDT) :: Nil)

  override def bufferSchema: StructType = StructType(
    StructField("Union", new GeometryUDT) :: Nil
  )

  override def dataType: DataType = new GeometryUDT

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    val coordinates: Array[Coordinate] = new Array[Coordinate](5)
    coordinates(0) = new Coordinate(-999999999, -999999999)
    coordinates(1) = new Coordinate(-999999999, -999999999)
    coordinates(2) = new Coordinate(-999999999, -999999999)
    coordinates(3) = new Coordinate(-999999999, -999999999)
    coordinates(4) = coordinates(0)
    val geometryFactory = new GeometryFactory()
    buffer(0) = geometryFactory.createPolygon(coordinates)
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val accumulateUnion = buffer.getAs[Polygon](0)
    val newPolygon = input.getAs[Polygon](0)
    if (accumulateUnion.getArea == 0) buffer(0) = newPolygon
    else buffer(0) = accumulateUnion.union(newPolygon)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val leftPolygon = buffer1.getAs[Polygon](0)
    val rightPolygon = buffer2.getAs[Polygon](0)
    if (leftPolygon.getCoordinates()(0).x == -999999999) buffer1(0) = rightPolygon
    else if (rightPolygon.getCoordinates()(0).x == -999999999) buffer1(0) = leftPolygon
    else buffer1(0) = leftPolygon.union(rightPolygon)
  }

  override def evaluate(buffer: Row): Any = {
    return buffer.getAs[Geometry](0)
  }
}

/**
  * Return the envelope boundary of the entire column
  */
class ST_Envelope_Aggr extends UserDefinedAggregateFunction {
  // This is the input fields for your aggregate function.
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("Envelope", new GeometryUDT) :: Nil)

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType = StructType(
    StructField("Envelope", new GeometryUDT) :: Nil
  )

  // This is the output type of your aggregatation function.
  override def dataType: DataType = new GeometryUDT

  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    val coordinates: Array[Coordinate] = new Array[Coordinate](5)
    coordinates(0) = new Coordinate(-999999999, -999999999)
    coordinates(1) = new Coordinate(-999999999, -999999999)
    coordinates(2) = new Coordinate(-999999999, -999999999)
    coordinates(3) = new Coordinate(-999999999, -999999999)
    coordinates(4) = new Coordinate(-999999999, -999999999)
    val geometryFactory = new GeometryFactory()
    buffer(0) = geometryFactory.createPolygon(coordinates)
    //buffer(0) = new GenericArrayData(GeometrySerializer.serialize(geometryFactory.createPolygon(coordinates)))
  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val accumulateEnvelope = buffer.getAs[Geometry](0).getEnvelopeInternal
    val newEnvelope = input.getAs[Geometry](0).getEnvelopeInternal
    val coordinates: Array[Coordinate] = new Array[Coordinate](5)
    var minX = 0.0
    var minY = 0.0
    var maxX = 0.0
    var maxY = 0.0
    if (accumulateEnvelope.getMinX == -999999999) {
      // Found the accumulateEnvelope is the initial value
      minX = newEnvelope.getMinX
      minY = newEnvelope.getMinY
      maxX = newEnvelope.getMaxX
      maxY = newEnvelope.getMaxY
    }
    else {
      minX = Math.min(accumulateEnvelope.getMinX, newEnvelope.getMinX)
      minY = Math.min(accumulateEnvelope.getMinY, newEnvelope.getMinY)
      maxX = Math.max(accumulateEnvelope.getMaxX, newEnvelope.getMaxX)
      maxY = Math.max(accumulateEnvelope.getMaxY, newEnvelope.getMaxY)
    }
    coordinates(0) = new Coordinate(minX, minY)
    coordinates(1) = new Coordinate(minX, maxY)
    coordinates(2) = new Coordinate(maxX, maxY)
    coordinates(3) = new Coordinate(maxX, minY)
    coordinates(4) = coordinates(0)
    val geometryFactory = new GeometryFactory()
    buffer(0) = geometryFactory.createPolygon(coordinates)
  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val leftEnvelope = buffer1.getAs[Geometry](0).getEnvelopeInternal
    val rightEnvelope = buffer2.getAs[Geometry](0).getEnvelopeInternal
    val coordinates: Array[Coordinate] = new Array[Coordinate](5)
    var minX = 0.0
    var minY = 0.0
    var maxX = 0.0
    var maxY = 0.0
    if (leftEnvelope.getMinX == -999999999) {
      // Found the leftEnvelope is the initial value
      minX = rightEnvelope.getMinX
      minY = rightEnvelope.getMinY
      maxX = rightEnvelope.getMaxX
      maxY = rightEnvelope.getMaxY
    }
    else if (rightEnvelope.getMinX == -999999999) {
      // Found the rightEnvelope is the initial value
      minX = leftEnvelope.getMinX
      minY = leftEnvelope.getMinY
      maxX = leftEnvelope.getMaxX
      maxY = leftEnvelope.getMaxY
    }
    else {
      minX = Math.min(leftEnvelope.getMinX, rightEnvelope.getMinX)
      minY = Math.min(leftEnvelope.getMinY, rightEnvelope.getMinY)
      maxX = Math.max(leftEnvelope.getMaxX, rightEnvelope.getMaxX)
      maxY = Math.max(leftEnvelope.getMaxY, rightEnvelope.getMaxY)
    }
    coordinates(0) = new Coordinate(minX, minY)
    coordinates(1) = new Coordinate(minX, maxY)
    coordinates(2) = new Coordinate(maxX, maxY)
    coordinates(3) = new Coordinate(maxX, minY)
    coordinates(4) = coordinates(0)
    val geometryFactory = new GeometryFactory()
    buffer1(0) = geometryFactory.createPolygon(coordinates)
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    return buffer.getAs[Geometry](0)
  }
}

class CB_MergeId extends UserDefinedAggregateFunction {

  // Defind the schema of the input data
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("id", StringType) :: Nil)

  // Define how the aggregates types will be
  override def bufferSchema: StructType = StructType(
    StructField("tails", StringType) :: Nil
  )

  // define the return type
  override def dataType: DataType = StringType

  // Does the function return the same value for the same input?
  override def deterministic: Boolean = true

  // Initial values
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = ""
  }

  // Updated based on Input
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getString(0) + "," + input.getString(0)
  }

  // Merge two schemas
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getString(0) + buffer2.getString(0)
  }

  // Output
  override def evaluate(buffer: Row): Any = {
    buffer.getString(0).drop(1)
  }
}


class CB_MergeDouble extends UserDefinedAggregateFunction with Logging with GlobalVariables{

  // Defind the schema of the input data
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("id", DoubleType) :: Nil)

  // Define how the aggregates types will be
  override def bufferSchema: StructType = StructType(
    StructField("tails", StringType) :: Nil
  )

  // define the return type
  override def dataType: DataType = StringType

  // Does the function return the same value for the same input?
  override def deterministic: Boolean = true

  // Initial values
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = ""
  }

  // Updated based on Input
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    try {
      buffer(0) = buffer.getString(0) + "," + input.getDouble(0)
    } catch {
      case e1:java.lang.NullPointerException => {
        buffer(0) = buffer.getString(0)
        log.warn(cubeLogPrefix+"CB_MergeDouble met a null")
      }
      case e2:java.lang.NumberFormatException => {
        buffer(0) = buffer.getString(0)
        log.warn(cubeLogPrefix+"CB_MergeDouble met a string")
      }
    }
  }

  // Merge two schemas
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getString(0) + buffer2.getString(0)
  }

  // Output
  override def evaluate(buffer: Row): Any = {
    buffer.getString(0).drop(1)
  }
}

/**
  * Current merge limit is 5000
  */
class CB_MergeIdLimit extends UserDefinedAggregateFunction {

  // Defind the schema of the input data
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("id", StringType) :: Nil)

  // Define how the aggregates types will be
  override def bufferSchema: StructType = new StructType()
    .add("tails", StringType)
    .add("count", LongType)

  // define the return type
  override def dataType: DataType = StringType

  // Does the function return the same value for the same input?
  override def deterministic: Boolean = true

  // Initial values
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = ""
    buffer(1) = 0L
  }

  // Updated based on Input
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (buffer.getLong(1) < 5000) buffer(0) = buffer.getString(0) + "," + input.getString(0);buffer(1) = buffer.getLong(1)+1
  }

  // Merge two schemas
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getString(0) + buffer2.getString(0)
    if (buffer1.getLong(1) < 5000) buffer1(0) = buffer1.getString(0) + buffer2.getString(0);buffer1(1) = buffer1.getLong(1)+buffer2.getLong(1)
  }

  // Output
  override def evaluate(buffer: Row): Any = {
    buffer.getString(0).drop(1)
  }
}

case class CB_EuclideanSum(child: Expression, threshold: Expression)
  extends  DeclarativeAggregate  {
  override def children: Seq[Expression] = Seq(child, threshold)

  override def nullable: Boolean = false
  override def dataType: DataType = BooleanType

  private lazy val belowThreshold = AttributeReference(
    "belowThreshold", BooleanType, nullable = false
  )()

  // Used to derive schema
  override lazy val aggBufferAttributes = belowThreshold :: Nil

  override lazy val initialValues = Seq(
    Literal(false)
  )

  override lazy val updateExpressions = Seq(Or(
    belowThreshold,
    If(IsNull(child), Literal(false), LessThan(child, threshold))
  ))

  override lazy val mergeExpressions = Seq(
    Or(belowThreshold.left, belowThreshold.right)
  )

  override lazy val evaluateExpression = belowThreshold
  override def defaultResult: Option[Literal] = Option(Literal(false))
}

class CB_MergeSpatial extends UserDefinedAggregateFunction {

  // Defind the schema of the input data
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("object", new SimplePointUDT) :: Nil)

  // Define how the aggregates types will be
  override def bufferSchema: StructType = StructType(
    StructField("mergestring", StringType) :: Nil
  )

  // define the return type
  override def dataType: DataType = StringType

  // Does the function return the same value for the same input?
  override def deterministic: Boolean = true

  // Initial values
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = ""
  }

  // Updated based on Input
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val coordinate = input.getAs[SimplePoint](0)
    buffer(0) = buffer.getString(0) + "," + coordinate.x+" "+coordinate.y
  }

  // Merge two schemas
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getString(0) + buffer2.getString(0)
  }

  // Output
  override def evaluate(buffer: Row): Any = {
    buffer.getString(0).drop(1)
  }
}