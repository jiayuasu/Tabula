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
package org.datasyslab.samplingcube.utils

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.datasyslab.samplingcube.GlobalVariables

/*
This is a math wrapper for Apache Commons Math. The original math functions may produce NaN value if the input array only has one value.
 */
object MathWrapper extends GlobalVariables {

  def avg(inputArray: Array[Double]): Double = {
    val cal = new DescriptiveStatistics(inputArray)
    val result = cal.getMean
    if (result.toString.equalsIgnoreCase("NaN")) inputArray(0)
    else result
  }

  def stddev(inputArray: Array[Double]): Double = {
    val cal = new DescriptiveStatistics(inputArray)
    val result = cal.getStandardDeviation
    if (result.isNaN) 0.0
    else result
  }

  def variance(inputArray: Array[Double]): Double = {
    val cal = new DescriptiveStatistics(inputArray)
    val result = cal.getVariance
    if (result.isNaN) 0.0
    else result
  }

  def skewness(inputArray: Array[Double]): Double = {
    val cal = new DescriptiveStatistics(inputArray)
    val result = cal.getSkewness
    if (result.isNaN) 0.0
    else result
  }

  def kurtosis(inputArray: Array[Double]): Double = {
    val cal = new DescriptiveStatistics(inputArray)
    val result = cal.getKurtosis
    if (result.isNaN) 0.0
    else result
  }
}
