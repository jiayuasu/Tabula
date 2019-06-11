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

package org.datasyslab.geosparksql

import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory, Polygon}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import org.scalatest.{BeforeAndAfterAll, FunSpec}

class aggregateFunctionTestScala extends FunSpec with BeforeAndAfterAll {

  var sparkSession: SparkSession = _

  override def afterAll(): Unit = {
    //GeoSparkSQLRegistrator.dropAll(sparkSession)
    //sparkSession.stop
  }

  describe("GeoSpark-SQL Aggregate Function Test") {
    sparkSession = SparkSession.builder().config("spark.serializer", classOf[KryoSerializer].getName).
      config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName).
      master("local[*]").appName("readTestScala").getOrCreate()
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    GeoSparkSQLRegistrator.registerAll(sparkSession.sqlContext)

    val resourceFolder = System.getProperty("user.dir") + "/src/test/resources/"

    val csvPolygonInputLocation = resourceFolder + "testunion.csv"
    val plainPointInputLocation = resourceFolder + "testpoint.csv"

    it("Passed ST_Envelope_aggr") {
      var pointCsvDF = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(plainPointInputLocation)
      pointCsvDF.createOrReplaceTempView("pointtable")
      var pointDf = sparkSession.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)), cast(pointtable._c1 as Decimal(24,20))) as arealandmark from pointtable")
      pointDf.createOrReplaceTempView("pointdf")
      var boundary = sparkSession.sql("select ST_Envelope_Aggr(pointdf.arealandmark) from pointdf")
      val coordinates: Array[Coordinate] = new Array[Coordinate](5)
      coordinates(0) = new Coordinate(1.1, 101.1)
      coordinates(1) = new Coordinate(1.1, 1100.1)
      coordinates(2) = new Coordinate(1000.1, 1100.1)
      coordinates(3) = new Coordinate(1000.1, 101.1)
      coordinates(4) = coordinates(0)
      val geometryFactory = new GeometryFactory()
      geometryFactory.createPolygon(coordinates)
      assert(boundary.take(1)(0).get(0) == geometryFactory.createPolygon(coordinates))
    }

    it("Passed ST_Union_aggr") {

      var polygonCsvDf = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(csvPolygonInputLocation)
      polygonCsvDf.createOrReplaceTempView("polygontable")
      polygonCsvDf.show()
      var polygonDf = sparkSession.sql("select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")
      polygonDf.show()
      var union = sparkSession.sql("select ST_Union_Aggr(polygondf.polygonshape) from polygondf")
      assert(union.take(1)(0).get(0).asInstanceOf[Polygon].getArea == 10100)
    }
  }
}
