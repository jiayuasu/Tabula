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

import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.geosparksql.UDT.{GeometryUDT, SimplePointUDT}
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.unsafe.types.UTF8String
import org.datasyslab.geospark.enums.{FileDataSplitter, GeometryType}
import org.datasyslab.geospark.formatMapper.FormatMapper
import org.datasyslab.geosparksql.utils.GeometrySerializer

/**
  * Return a point from a string. The string must be plain string and each coordinate must be separated by a delimiter.
  *
  * @param inputExpressions This function takes 2 parameters. The first parameter is the input geometry
  *                         string, the second parameter is the delimiter. String format should be similar to CSV/TSV
  */
case class ST_PointFromText(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes two input expressions.
    assert(inputExpressions.length == 2)
    val geomString = inputExpressions(0).eval(inputRow).asInstanceOf[UTF8String].toString
    val geomFormat = inputExpressions(1).eval(inputRow).asInstanceOf[UTF8String].toString
    var fileDataSplitter = FileDataSplitter.getFileDataSplitter(geomFormat)
    var formatMapper = new FormatMapper(fileDataSplitter, false, GeometryType.POINT)
    var geometry = formatMapper.readGeometry(geomString)
    return new GenericArrayData(GeometrySerializer.serialize(geometry))
  }

  override def dataType: DataType = new GeometryUDT()

  override def children: Seq[Expression] = inputExpressions
}

/**
  * Return a polygon from a string. The string must be plain string and each coordinate must be separated by a delimiter.
  *
  * @param inputExpressions
  */
case class ST_PolygonFromText(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes two input expressions.
    assert(inputExpressions.length == 2)
    val geomString = inputExpressions(0).eval(inputRow).asInstanceOf[UTF8String].toString
    val geomFormat = inputExpressions(1).eval(inputRow).asInstanceOf[UTF8String].toString

    var fileDataSplitter = FileDataSplitter.getFileDataSplitter(geomFormat)
    var formatMapper = new FormatMapper(fileDataSplitter, false, GeometryType.POLYGON)
    var geometry = formatMapper.readGeometry(geomString)
    return new GenericArrayData(GeometrySerializer.serialize(geometry))
  }

  override def dataType: DataType = new GeometryUDT()

  override def children: Seq[Expression] = inputExpressions
}

/**
  * Return a linestring from a string. The string must be plain string and each coordinate must be separated by a delimiter.
  *
  * @param inputExpressions
  */
case class ST_LineStringFromText(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes two input expressions.
    assert(inputExpressions.length == 2)

    val geomString = inputExpressions(0).eval(inputRow).asInstanceOf[UTF8String].toString
    val geomFormat = inputExpressions(1).eval(inputRow).asInstanceOf[UTF8String].toString

    var fileDataSplitter = FileDataSplitter.getFileDataSplitter(geomFormat)
    var formatMapper = new FormatMapper(fileDataSplitter, false, GeometryType.LINESTRING)
    var geometry = formatMapper.readGeometry(geomString)

    return new GenericArrayData(GeometrySerializer.serialize(geometry))
  }

  override def dataType: DataType = new GeometryUDT()

  override def children: Seq[Expression] = inputExpressions
}


/**
  * Return a Geometry from a WKT string
  *
  * @param inputExpressions This function takes 1 parameter which is the geometry string. The string format must be WKT.
  */
case class ST_GeomFromWKT(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    assert(inputExpressions.length == 1)
    val geomString = inputExpressions(0).eval(inputRow).asInstanceOf[UTF8String].toString
    var fileDataSplitter = FileDataSplitter.WKT
    var formatMapper = new FormatMapper(fileDataSplitter, false)
    var geometry = formatMapper.readGeometry(geomString)
    return new GenericArrayData(GeometrySerializer.serialize(geometry))
  }

  override def dataType: DataType = new GeometryUDT()

  override def children: Seq[Expression] = inputExpressions
}


/**
  * Return a Geometry from a WKB string
  *
  * @param inputExpressions This function takes 1 parameter which is the geometry string. The string format must be WKB.
  */
case class ST_GeomFromWKB(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    assert(inputExpressions.length == 1)
    val geomString = inputExpressions(0).eval(inputRow).asInstanceOf[UTF8String].toString
    var fileDataSplitter = FileDataSplitter.WKB
    var formatMapper = new FormatMapper(fileDataSplitter, false)
    var geometry = formatMapper.readGeometry(geomString)
    return new GenericArrayData(GeometrySerializer.serialize(geometry))
  }

  override def dataType: DataType = new GeometryUDT()

  override def children: Seq[Expression] = inputExpressions
}

/**
  * Return a Point from X and Y
  *
  * @param inputExpressions This function takes 2 parameter which are point x and y.
  */
case class ST_Point(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    assert(inputExpressions.length == 2)
    val x = inputExpressions(0).eval(inputRow).asInstanceOf[Decimal].toDouble
    val y = inputExpressions(1).eval(inputRow).asInstanceOf[Decimal].toDouble
    var geometryFactory = new GeometryFactory()
    var geometry = geometryFactory.createPoint(new Coordinate(x, y))
    return new GenericArrayData(GeometrySerializer.serialize(geometry))
  }

  override def dataType: DataType = new GeometryUDT()

  override def children: Seq[Expression] = inputExpressions
}


/**
  * Return a polygon given minX,minY,maxX,maxY
  *
  * @param inputExpressions
  */
case class ST_PolygonFromEnvelope(inputExpressions: Seq[Expression]) extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    assert(inputExpressions.length == 4)
    val minX = inputExpressions(0).eval(input).asInstanceOf[Decimal].toDouble
    val minY = inputExpressions(1).eval(input).asInstanceOf[Decimal].toDouble
    val maxX = inputExpressions(2).eval(input).asInstanceOf[Decimal].toDouble
    val maxY = inputExpressions(3).eval(input).asInstanceOf[Decimal].toDouble
    var coordinates = new Array[Coordinate](5)
    coordinates(0) = new Coordinate(minX, minY)
    coordinates(1) = new Coordinate(minX, maxY)
    coordinates(2) = new Coordinate(maxX, maxY)
    coordinates(3) = new Coordinate(maxX, minY)
    coordinates(4) = coordinates(0)
    val geometryFactory = new GeometryFactory()
    val polygon = geometryFactory.createPolygon(coordinates)
    new GenericArrayData(GeometrySerializer.serialize(polygon))
  }

  override def dataType: DataType = new GeometryUDT()

  override def children: Seq[Expression] = inputExpressions
}

case class CB_SimplePointFromWKT(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    assert(inputExpressions.length == 1)
    val geomString = inputExpressions(0).eval(inputRow).asInstanceOf[UTF8String].toString
    var fileDataSplitter = FileDataSplitter.WKT
    var formatMapper = new FormatMapper(fileDataSplitter, false)
    var coordinate = formatMapper.readGeometry(geomString).getCoordinate
    val output = new Array[Any](2)
    output(0) = BigDecimal(coordinate.x).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble
    output(1) = BigDecimal(coordinate.y).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble
    return new GenericArrayData(output)
  }

  override def dataType: DataType = new SimplePointUDT()

  override def children: Seq[Expression] = inputExpressions
}

trait UserDataGeneratator {
  def generateUserData(minInputLength: Integer, inputExpressions: Seq[Expression], inputRow: InternalRow): String = {
    var userData = inputExpressions(minInputLength).eval(inputRow).asInstanceOf[UTF8String].toString

    for (i <- minInputLength + 1 to inputExpressions.length - 1) {
      userData = userData + "\t" + inputExpressions(i).eval(inputRow).asInstanceOf[UTF8String].toString
    }
    return userData
  }
}

