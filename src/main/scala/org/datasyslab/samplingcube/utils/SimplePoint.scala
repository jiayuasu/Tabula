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
package org.datasyslab.samplingcube.utils

//@SQLUserDefinedType(udt = classOf[SimplePointUDT])
class SimplePoint(val x:Double, val y:Double) extends Serializable {
  override def hashCode: Int = 41 * (41 + x.toInt) + y.toInt
  override def equals(that: Any): Boolean = {
    if (that.isInstanceOf[SimplePoint]) {
      val e = that.asInstanceOf[SimplePoint]
      (this.x == e.x || (this.x.isNaN && e.x.isNaN) || (this.x.isInfinity && e.x.isInfinity)) &&
        (this.y == e.y || (this.y.isNaN && e.y.isNaN) || (this.y.isInfinity && e.y.isInfinity))
    } else {
      false
    }
  }
  override def toString: String = x.toString+" "+y.toString
}
