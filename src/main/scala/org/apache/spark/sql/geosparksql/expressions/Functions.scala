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
package org.apache.spark.sql.geosparksql.expressions

import com.vividsolutions.jts.geom.PrecisionModel
import com.vividsolutions.jts.operation.valid.IsValidOp
import com.vividsolutions.jts.precision.GeometryPrecisionReducer
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.geosparksql.UDT.GeometryUDT
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.datasyslab.geosparksql.utils.GeometrySerializer
import org.datasyslab.samplingcube.GlobalVariables
import org.datasyslab.samplingcube.utils.{MathWrapper, SerializableUdf, SimplePoint}
import org.geotools.geometry.jts.JTS
import org.geotools.referencing.CRS
import org.opengis.referencing.operation.MathTransform

/**
  * Return the distance between two geometries.
  *
  * @param inputExpressions This function takes two geometries and calculates the distance between two objects.
  */
case class ST_Distance(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback {

  // This is a binary expression
  assert(inputExpressions.length == 2)

  override def nullable: Boolean = false

  override def toString: String = s" **${ST_Distance.getClass.getName}**  "

  override def children: Seq[Expression] = inputExpressions

  override def eval(inputRow: InternalRow): Any = {
    assert(inputExpressions.length == 2)

    val leftArray = inputExpressions(0).eval(inputRow).asInstanceOf[ArrayData]
    val rightArray = inputExpressions(1).eval(inputRow).asInstanceOf[ArrayData]

    val leftGeometry = GeometrySerializer.deserialize(leftArray)

    val rightGeometry = GeometrySerializer.deserialize(rightArray)

    return leftGeometry.distance(rightGeometry)
  }

  override def dataType = DoubleType
}

/**
  * Return the convex hull of a Geometry.
  *
  * @param inputExpressions
  */
case class ST_ConvexHull(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback {
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    assert(inputExpressions.length == 1)
    val geometry = GeometrySerializer.deserialize(inputExpressions(0).eval(input).asInstanceOf[ArrayData])
    new GenericArrayData(GeometrySerializer.serialize(geometry.convexHull()))
  }

  override def dataType: DataType = new GeometryUDT()

  override def children: Seq[Expression] = inputExpressions
}

/**
  * Return the bounding rectangle for a Geometry
  *
  * @param inputExpressions
  */
case class ST_Envelope(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback {
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    assert(inputExpressions.length == 1)
    val geometry = GeometrySerializer.deserialize(inputExpressions(0).eval(input).asInstanceOf[ArrayData])
    new GenericArrayData(GeometrySerializer.serialize(geometry.getEnvelope()))
  }

  override def dataType: DataType = new GeometryUDT()

  override def children: Seq[Expression] = inputExpressions
}

/**
  * Return the length measurement of a Geometry
  *
  * @param inputExpressions
  */
case class ST_Length(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback {
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    assert(inputExpressions.length == 1)
    val geometry = GeometrySerializer.deserialize(inputExpressions(0).eval(input).asInstanceOf[ArrayData])
    return geometry.getLength
  }

  override def dataType: DataType = DoubleType

  override def children: Seq[Expression] = inputExpressions
}

/**
  * Return the area measurement of a Geometry.
  *
  * @param inputExpressions
  */
case class ST_Area(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback {
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    assert(inputExpressions.length == 1)
    val geometry = GeometrySerializer.deserialize(inputExpressions(0).eval(input).asInstanceOf[ArrayData])
    return geometry.getArea
  }

  override def dataType: DataType = DoubleType

  override def children: Seq[Expression] = inputExpressions
}

/**
  * Return mathematical centroid of a geometry.
  *
  * @param inputExpressions
  */
case class ST_Centroid(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback {
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    assert(inputExpressions.length == 1)
    val geometry = GeometrySerializer.deserialize(inputExpressions(0).eval(input).asInstanceOf[ArrayData])
    new GenericArrayData(GeometrySerializer.serialize(geometry.getCentroid()))
  }

  override def dataType: DataType = new GeometryUDT()

  override def children: Seq[Expression] = inputExpressions
}

/**
  * Given a geometry, sourceEPSGcode, and targetEPSGcode, convert the geometry's Spatial Reference System / Coordinate Reference System.
  *
  * @param inputExpressions
  */
case class ST_Transform(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback {
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    assert(inputExpressions.length >= 3 && inputExpressions.length <= 5)
    System.setProperty("org.geotools.referencing.forceXY", "true")
    if (inputExpressions.length >= 4) {
      System.setProperty("org.geotools.referencing.forceXY", inputExpressions(3).eval(input).asInstanceOf[Boolean].toString)
    }
    val originalGeometry = GeometrySerializer.deserialize(inputExpressions(0).eval(input).asInstanceOf[ArrayData])
    val sourceCRScode = CRS.decode(inputExpressions(1).eval(input).asInstanceOf[UTF8String].toString)
    val targetCRScode = CRS.decode(inputExpressions(2).eval(input).asInstanceOf[UTF8String].toString)
    var transform: MathTransform = null
    if (inputExpressions.length == 5) {
      transform = CRS.findMathTransform(sourceCRScode, targetCRScode, inputExpressions(4).eval(input).asInstanceOf[Boolean])
    }
    else {
      transform = CRS.findMathTransform(sourceCRScode, targetCRScode, false)
    }
    new GenericArrayData(GeometrySerializer.serialize(JTS.transform(originalGeometry, transform)))
  }

  override def dataType: DataType = new GeometryUDT()

  override def children: Seq[Expression] = inputExpressions
}

/**
  * Return the intersection shape of two geometries. The return type is a geometry
  *
  * @param inputExpressions
  */
case class ST_Intersection(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback {
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    assert(inputExpressions.length == 2)
    val leftgeometry = GeometrySerializer.deserialize(inputExpressions(0).eval(input).asInstanceOf[ArrayData])
    val rightgeometry = GeometrySerializer.deserialize(inputExpressions(1).eval(input).asInstanceOf[ArrayData])
    new GenericArrayData(GeometrySerializer.serialize(leftgeometry.intersection(rightgeometry)))
  }

  override def dataType: DataType = new GeometryUDT()

  override def children: Seq[Expression] = inputExpressions
}

/**
  * Test if Geometry is valid.
  *
  * @param inputExpressions
  */
case class ST_IsValid(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback {
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    assert(inputExpressions.length == 1)
    val geometry = GeometrySerializer.deserialize(inputExpressions(0).eval(input).asInstanceOf[ArrayData])
    val isvalidop = new IsValidOp(geometry)
    isvalidop.isValid
  }

  override def dataType: DataType = BooleanType

  override def children: Seq[Expression] = inputExpressions
}

/**
  * Reduce the precision of the given geometry to the given number of decimal places
  *
  * @param inputExpressions The first arg is a geom and the second arg is an integer scale, specifying the number of decimal places of the new coordinate. The last decimal place will
  *                         be rounded to the nearest number.
  */
case class ST_PrecisionReduce(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback {
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    assert(inputExpressions.length == 2)
    val geometry = GeometrySerializer.deserialize(inputExpressions(0).eval(input).asInstanceOf[ArrayData])
    val precisionScale = inputExpressions(1).eval(input).asInstanceOf[Int]
    val precisionReduce = new GeometryPrecisionReducer(new PrecisionModel(Math.pow(10, precisionScale)))
    new GenericArrayData(GeometrySerializer.serialize(precisionReduce.reduce(geometry)))
  }

  override def dataType: DataType = new GeometryUDT()

  override def children: Seq[Expression] = inputExpressions
}

case class CB_Loss(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with Logging with SerializableUdf {
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    assert(inputExpressions.length == 2)
    calculateLoss(inputExpressions(0).eval(input).asInstanceOf[Double], inputExpressions(1).eval(input).asInstanceOf[Double])
  }

  override def dataType: DataType = DoubleType

  override def children: Seq[Expression] = inputExpressions
}

case class CB_Threshold(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with Logging with SerializableUdf {
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    assert(inputExpressions.length == 2)
    return threshold(inputExpressions(0).eval(input).asInstanceOf[Double], Seq(inputExpressions(1).eval(input).asInstanceOf[Double]))
  }

  override def dataType: DataType = DoubleType

  override def children: Seq[Expression] = inputExpressions
}

case class CB_ArrayAvg(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with Logging with GlobalVariables {
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    assert(inputExpressions.length == 1)
    try {
      val sample = inputExpressions(0).eval(input).asInstanceOf[ArrayData].toDoubleArray()
      MathWrapper.avg(sample)
    }
    catch {
      case e: NullPointerException => InvalidMeasureValue
    }
  }

  override def dataType: DataType = DoubleType

  override def children: Seq[Expression] = inputExpressions
}

case class CB_ArrayStddev(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with Logging with GlobalVariables {
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    assert(inputExpressions.length == 1)
    try {
      val sample = inputExpressions(0).eval(input).asInstanceOf[ArrayData].toDoubleArray()
      MathWrapper.stddev(sample)
    }
    catch {
      case e: NullPointerException => InvalidMeasureValue
    }
  }

  override def dataType: DataType = DoubleType

  override def children: Seq[Expression] = inputExpressions
}

case class CB_ArraySkewness(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with Logging with GlobalVariables {
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    assert(inputExpressions.length == 1)
    try {
      val sample = inputExpressions(0).eval(input).asInstanceOf[ArrayData].toDoubleArray()
      MathWrapper.skewness(sample)
    }
    catch {
      case e: NullPointerException => InvalidMeasureValue
    }
  }

  override def dataType: DataType = DoubleType

  override def children: Seq[Expression] = inputExpressions
}

case class CB_ArrayKurtosis(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with Logging with GlobalVariables {
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    assert(inputExpressions.length == 1)
    try {
      val sample = inputExpressions(0).eval(input).asInstanceOf[ArrayData].toDoubleArray()
      MathWrapper.kurtosis(sample)
    }
    catch {
      case e: NullPointerException => InvalidMeasureValue
    }
  }

  override def dataType: DataType = DoubleType

  override def children: Seq[Expression] = inputExpressions
}

case class CB_ArrayVariance(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with Logging with GlobalVariables {
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    assert(inputExpressions.length == 1)
    try {
      val sample = inputExpressions(0).eval(input).asInstanceOf[ArrayData].toDoubleArray()
      MathWrapper.variance(sample)
    }
    catch {
      case e: NullPointerException => InvalidMeasureValue
    }
  }

  override def dataType: DataType = DoubleType

  override def children: Seq[Expression] = inputExpressions
}

case class CB_EuclideanSum_1D(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with Logging with SerializableUdf {
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    assert(inputExpressions.length == 2)
    var rawDataSet = inputExpressions(0).eval(input).asInstanceOf[UTF8String].toString.split(",").map(f=>f.toDouble)
    var sampleSet = inputExpressions(1).eval(input).asInstanceOf[UTF8String].toString.split(",").map(f=>f.toDouble)
    euclideansum_1D(rawDataSet, sampleSet)
  }

  override def dataType: DataType = DoubleType

  override def children: Seq[Expression] = inputExpressions
}

case class CB_EuclideanSum_Spatial(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with Logging with SerializableUdf {
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    assert(inputExpressions.length == 2)
    var rawDataSet = inputExpressions(0).eval(input).asInstanceOf[UTF8String].toString.split(",").map(f=>toSimplePoint(f))
    var sampleSet = inputExpressions(1).eval(input).asInstanceOf[UTF8String].toString.split(",").map(f=>toSimplePoint(f))
    euclideansum_spatial(rawDataSet, sampleSet)
  }

  override def dataType: DataType = DoubleType

  override def children: Seq[Expression] = inputExpressions
}

case class CB_Sampling_1D(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with Logging with SerializableUdf {
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    assert(inputExpressions.length == 2)
    new GenericArrayData(sampling_1D_EucOpt(inputExpressions(0).eval(input).asInstanceOf[UTF8String].toString.split(",").map(f=>f.toDouble)
      , inputExpressions(1).eval(input).asInstanceOf[Decimal].toDouble))
  }

  override def dataType: DataType = ArrayType(DoubleType, false)

  override def children: Seq[Expression] = inputExpressions
}

case class CB_Sampling_Spatial(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with Logging with SerializableUdf {
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    assert(inputExpressions.length == 2)
    new GenericArrayData(sampling_spatial_EucOpt(inputExpressions(0).eval(input).asInstanceOf[UTF8String].toString.split(",").map(f=>{
      val coordinates = f.split(" ")
      new SimplePoint(coordinates(0).toDouble, coordinates(1).toDouble)
    })
      , inputExpressions(1).eval(input).asInstanceOf[Decimal].toDouble, true).map(f=>UTF8String.fromString(f.toString)))
  }

  override def dataType: DataType = ArrayType(StringType, false)

  override def children: Seq[Expression] = inputExpressions
}

case class CB_EuclideanLoss_1D(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with Logging with SerializableUdf {
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    assert(inputExpressions.length == 2)
    var rawDataSet = inputExpressions(0).eval(input).asInstanceOf[UTF8String].toString.split(",").filterNot(f=>f.equalsIgnoreCase("na"))
      .map(f=>f.toDouble)
    var sampleSet = inputExpressions(1).eval(input).asInstanceOf[UTF8String].toString.split(",").filterNot(f=>f.equalsIgnoreCase("na"))
      .map(f=>f.toDouble)
    euclideansum_1D(rawDataSet, sampleSet)*1.0/rawDataSet.length
  }

  override def dataType: DataType = DoubleType

  override def children: Seq[Expression] = inputExpressions
}

case class CB_EuclideanLoss_Spatial(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with Logging with SerializableUdf {
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    assert(inputExpressions.length == 2)
    var rawDataSet = inputExpressions(0).eval(input).asInstanceOf[UTF8String].toString.split(",")//.filterNot(f=>f.equalsIgnoreCase("na"))
      .map(f=>toSimplePoint(f))
    var sampleSet = inputExpressions(1).eval(input).asInstanceOf[UTF8String].toString.split(",")//.filterNot(f=>f.equalsIgnoreCase("na"))
      .map(f=>toSimplePoint(f))
    euclideansum_spatial(rawDataSet, sampleSet)*1.0/rawDataSet.length
  }

  override def dataType: DataType = DoubleType

  override def children: Seq[Expression] = inputExpressions
}

case class CB_Min_Distance_Spatial(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with Logging with SerializableUdf {
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    assert(inputExpressions.length == 2)
    var rawArray = inputExpressions(0).eval(input).asInstanceOf[ArrayData].toDoubleArray()
    var rawObject = new SimplePoint(rawArray(0), rawArray(1))
    var sampleSet = inputExpressions(1).eval(input).asInstanceOf[UTF8String].toString.split(",").map(f=>toSimplePoint(f))
    min_distance_Spatial(rawObject, sampleSet)
  }

  override def dataType: DataType = DoubleType

  override def children: Seq[Expression] = inputExpressions
}