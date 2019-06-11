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
package org.apache.spark.sql.geosparksql.UDT

import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.index.SpatialIndex
import org.apache.spark.sql.types.UDTRegistration
import org.datasyslab.samplingcube.utils.SimplePoint

object UdtRegistratorWrapper {
  def registerAll(): Unit = {
    UDTRegistration.register(classOf[Geometry].getName, classOf[GeometryUDT].getName)
    UDTRegistration.register(classOf[SpatialIndex].getName, classOf[IndexUDT].getName)
    UDTRegistration.register(classOf[SimplePoint].getName, classOf[SimplePointUDT].getName)
  }
}
