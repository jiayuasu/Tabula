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
package org.apache.spark.sql.geosparksql.UDT

import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types._
import org.datasyslab.samplingcube.utils.SimplePoint

/**
  * User-defined type for [[SimplePoint]].
  */
private[sql] class SimplePointUDT extends UserDefinedType[SimplePoint] {

  override def sqlType: DataType = ArrayType(DoubleType, false)

  //override def pyUDT: String = "pyspark.sql.tests.SimplePointUDT"

  override def serialize(p: SimplePoint): GenericArrayData = {
    val output = new Array[Double](2)
    output(0) = p.x
    output(1) = p.y
    new GenericArrayData(output)
  }

  override def deserialize(datum: Any): SimplePoint = {
    datum match {
      case values: ArrayData =>
        new SimplePoint(values.getDouble(0), values.getDouble(1))
    }
  }

  override def userClass: Class[SimplePoint] = classOf[SimplePoint]

  //private[spark] override def asNullable: SimplePointUDT = this

  case object SimplePointUDT extends SimplePointUDT
}

