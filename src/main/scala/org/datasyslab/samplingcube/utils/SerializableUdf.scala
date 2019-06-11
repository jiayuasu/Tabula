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

import org.datasyslab.samplingcube.GlobalVariables

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.Random
import scala.util.control.Breaks._

trait SerializableUdf extends GlobalVariables{

  /**
    * UDF for loss
    *
    * @param dataMeasure
    * @param sampleMeasure
    * @return
    */
  def calculateLoss(dataMeasure: Double, sampleMeasure: Double): Double = {
    if (dataMeasure == 0.0) return 1.0
    var loss = Math.abs((dataMeasure - sampleMeasure) * 1.0 / dataMeasure)
    if (loss < 0.0001) 0.0 else if (loss > 1.0) 1.0 else loss
  }

  /**
    * UDF for threshold
    *
    * @param optimalLoss
    * @param threshold
    * @return
    */
  def threshold(globalLoss: Double, inputArgs: Seq[Double]): Double = {
    // Return a given threshold directly
    if (inputArgs.size == 1) return inputArgs(0)
    // Return a given threshold*percent directly
    if (inputArgs.size == 2) return inputArgs(0) * inputArgs(1)

    assert(inputArgs.size <= 3)

    return globalLoss
  }

  def euclideansum_1D(inputData:Array[Double], sample:Array[Double]): Double = {
    var sum = 0.0
    inputData.foreach(f=>{
      var minDist = -1.0
      sample.foreach(g=> {
        var distance = calculateNormDist_1D(f, g)
        if (minDist== -1.0) minDist = distance else if(distance<minDist) minDist = distance
      })
      sum = sum+minDist
    })
    sum
  }

  def calculateNormDist_1D(a:Double, b:Double): Double = {
    val normalizedDistance = Math.abs(a - b)
    if(normalizedDistance>1.0) 1.0 else normalizedDistance
    //    1 - 1.0/(1.0+Math.abs(a - b))
  }

  def calculateNormDist_Spatial(f:SimplePoint, g:SimplePoint): Double = {
    val normalizedDistance = Math.sqrt(Math.pow(f.x - g.x, 2)+Math.pow(f.y - g.y, 2))/maximumDistance
    if(normalizedDistance>1.0) 1.0 else normalizedDistance
//    1.0 - 1.0/(1.0 + Math.sqrt(Math.pow(f.x - g.x, 2)+Math.pow(f.y - g.y, 2)))
  }

  def euclideansum_spatial(inputData:Array[SimplePoint], sample:Array[SimplePoint]): Double = {
    var sum = 0.0
    inputData.foreach(f=>{
      sum = sum+min_distance_Spatial(f, sample)
    })
    sum
  }

  def min_distance_Spatial(rawObject: SimplePoint, sample:Array[SimplePoint]): Double = {
    var minDist = -1.0
    sample.foreach(g=> {
      var distance = calculateNormDist_Spatial(rawObject, g)
      if (minDist== -1.0) minDist = distance else if(distance<minDist) minDist = distance
    })
    minDist
  }

  def euclideanloss_1D(inputData:Array[Double], sample:Array[Double]): Double = {
    val sum = euclideansum_1D(inputData, sample)
    sum*1.0/inputData.length
  }


  def euclideanloss_spatial(inputData:Array[SimplePoint], sample:Array[SimplePoint]): Double = {
    val sum = euclideansum_spatial(inputData, sample)
    sum*1.0/inputData.length
  }

  /**
    * Greedy sampling function with lazy forward + optimization for euclidead 1d. This is 30% to 50% faster than the original lazy forward
    * @param inputData
    * @param threshold
    * @return
    */
  def sampling_1D_EucOpt(rawInputData:Array[Double], threshold:Double): Array[Double] = {
    // Sample the raw data to reduce the computation complexity
    var inputData = drawRandomSample(rawInputData, calSampleSize(rawInputData.length, epsilon, delta))
    var sample = new ArrayBuffer[Double]()
    var lastMinLoss = Array.fill(inputData.length)(-1.0)//List.fill(inputData.length)(-1.0).toBuffer
    var lastLoss = 1.0
    // value, margindecrease, greedyround
    def decrement(t3:(Double, Double, Int)) = t3._2
    var marginDecrementQueue = mutable.PriorityQueue[(Double, Double, Int)]()(Ordering.by(decrement))

    inputData.foreach(f=> {
      var sum = euclideansum_1D(inputData, Array(f))
      var curLoss = sum*1.0/inputData.length
      marginDecrementQueue.enqueue((f, lastLoss - curLoss, 1))
    })

    // Initialize the lastMinLoss
    val firstSample = marginDecrementQueue.dequeue()
    for (i <- inputData.indices) {
      lastMinLoss(i) = calculateNormDist_1D(inputData(i), firstSample._1)
    }
//    println(lastMinLoss.mkString(","))
    marginDecrementQueue.enqueue(firstSample)

    breakable {
      while (true) {
        var head = marginDecrementQueue.dequeue()
        while (head._3 != sample.length+1) {
//          sample.append(head._1)
//          var sum = euclideansum_1D(inputData, sample.toArray)
          var sum = 0.0
          (inputData zip lastMinLoss).foreach(f=>{
            val newMinLoss = calculateNormDist_1D(f._1, head._1)
            if (newMinLoss < f._2) {
              sum+=newMinLoss
            }
            else sum+=f._2
          })
          var curLoss = sum*1.0/inputData.length
          val marginDecrease = lastLoss - curLoss
//          marginDecrementQueue.enqueue((head._1, marginDecrease, sample.length))
          marginDecrementQueue.enqueue((head._1, marginDecrease, sample.length+1))
          head = marginDecrementQueue.dequeue()
//          sample.remove(sample.length-1)
        }
        sample.append(head._1)
        lastLoss -= head._2
        // Decide to add this data, update lastMinLoss
        var sum = 0.0
        for (j <- inputData.indices) {
          val newMinLoss = calculateNormDist_1D(inputData(j), head._1)
          if (newMinLoss < lastMinLoss(j)) {
            lastMinLoss(j) = newMinLoss
          }
          sum+=lastMinLoss(j)
        }
//        println(s"finish a greedy round, loss decrease ${head._2}")
        if (lastLoss <= threshold || marginDecrementQueue.isEmpty) {
//          println(s"quit loss = $lastLoss")
          break
        }
      }
    }
    sample.toArray
  }

  /**
    * Greedy sampling function for generic loss function
    * @param inputData
    * @param threshold
    * @return
    */
  def sampling_1D(rawInputData:Array[Double], threshold:Double): Array[Double] = {
    // Sample the raw data to reduce the computation complexity
    var inputData = drawRandomSample(rawInputData, calSampleSize(rawInputData.length, epsilon, delta))
    var sample = new ArrayBuffer[Double]()
    var lastMinLoss = List.fill(inputData.length)(-1.0).toBuffer
    var lastLoss = 1.0
    // value, margindecrease, greedyround
    def decrement(t3:(Double, Double, Int)) = t3._2
    var marginDecrementQueue = mutable.PriorityQueue[(Double, Double, Int)]()(Ordering.by(decrement))

    for (i <- inputData.indices) {
      val curLoss = euclideanloss_1D(inputData, Array(inputData(i)))
//      val curLoss = calculateLoss(MathWrapper.avg(inputData), inputData(i))
      marginDecrementQueue.enqueue((inputData(i), lastLoss - curLoss, 1))
    }

    breakable {
      while (true) {
        var head = marginDecrementQueue.dequeue()
        while (head._3 != sample.length+1) {
          sample.append(head._1)
          val curLoss = euclideanloss_1D(inputData, sample.toArray)
//          val curLoss =  calculateLoss(MathWrapper.avg(inputData), MathWrapper.avg(sample.toArray))
          //println(s"curloss = $curLoss")
          val marginDecrease = lastLoss - curLoss
          //          println(s"marginDecrease is $marginDecrease")
          marginDecrementQueue.enqueue((head._1, marginDecrease, sample.length))
          //          marginDecrementQueue.enqueue((head._1, marginDecrease, sample.length+1))
          //          println(marginDecrementQueue.mkString(","))
          head = marginDecrementQueue.dequeue()
          sample.remove(sample.length-1)
        }
        sample.append(head._1)
        lastLoss -= head._2
//        println(s"finish a greedy round, loss decrease ${head._2}")
        if (lastLoss <= threshold || marginDecrementQueue.isEmpty) {
//          println(s"quit loss = $lastLoss")
          break
        }
      }
    }
    sample.toArray
  }


  def sampling_spatial(rawInputData:Array[SimplePoint], threshold:Double): Array[SimplePoint] = {
    // Sample the raw data to reduce the computation complexity
    var inputData = drawRandomSample(rawInputData, calSampleSize(rawInputData.length, epsilon, delta))
    var sample = new ArrayBuffer[SimplePoint]()
    var lastMinLoss = List.fill(inputData.length)(-1.0).toBuffer
    var lastLoss = 1.0
    // value, margindecrease, greedyround
    def decrement(t3:(SimplePoint, Double, Int)) = t3._2
    var marginDecrementQueue = mutable.PriorityQueue[(SimplePoint, Double, Int)]()(Ordering.by(decrement))

    for (i <- inputData.indices) {
      val curLoss = euclideanloss_spatial(inputData, Array(inputData(i)))
      marginDecrementQueue.enqueue((inputData(i), lastLoss - curLoss, 1))
    }

    breakable {
      while (true) {
        var head = marginDecrementQueue.dequeue()
        while (head._3 != sample.length+1) {
          sample.append(head._1)
          val curLoss = euclideanloss_spatial(inputData, sample.toArray)
          //println(s"curloss = $curLoss")
          val marginDecrease = lastLoss - curLoss
          //          println(s"marginDecrease is $marginDecrease")
          marginDecrementQueue.enqueue((head._1, marginDecrease, sample.length))
          //          marginDecrementQueue.enqueue((head._1, marginDecrease, sample.length+1))
          //          println(marginDecrementQueue.mkString(","))
          head = marginDecrementQueue.dequeue()
          sample.remove(sample.length-1)
        }
        sample.append(head._1)
        lastLoss -= head._2
//        println(s"finish a greedy round, loss decrease ${head._2}")
        if (lastLoss <= threshold || marginDecrementQueue.isEmpty) {
//          println(s"quit loss = $lastLoss")
          break
        }
      }
    }
    sample.toArray
  }

  /**
    * Greedy sampling function with lazy forward + optimization for euclidead 1d. This is 30% to 50% faster than the original lazy forward
    * @param inputData
    * @param threshold
    * @return
    */
  def sampling_spatial_EucOpt(rawInputData:Array[SimplePoint], threshold:Double, drawSample:Boolean): Array[SimplePoint] = {
    // Sample the raw data to reduce the computation complexity
    var inputData = if (drawSample) {drawRandomSample(rawInputData, calSampleSize(rawInputData.length, epsilon, delta))} else {rawInputData}
    var sample = new ArrayBuffer[SimplePoint]()
    var lastMinLoss = Array.fill(inputData.length)(-1.0)//List.fill(inputData.length)(-1.0).toBuffer
    var lastLoss = 1.0
    // value, margindecrease, greedyround
    def decrement(t3:(SimplePoint, Double, Int)) = t3._2
    var marginDecrementQueue = mutable.PriorityQueue[(SimplePoint, Double, Int)]()(Ordering.by(decrement))

    inputData.foreach(f=> {
      var sum = euclideansum_spatial(inputData, Array(f))
      var curLoss = sum*1.0/inputData.length
      marginDecrementQueue.enqueue((f, lastLoss - curLoss, 1))
    })

    // Initialize the lastMinLoss
    val firstSample = marginDecrementQueue.dequeue()
    for (i <- inputData.indices) {
      lastMinLoss(i) = calculateNormDist_Spatial(inputData(i), firstSample._1)
    }
    //    println(lastMinLoss.mkString(","))
    marginDecrementQueue.enqueue(firstSample)

    breakable {
      while (true) {
        var head = marginDecrementQueue.dequeue()
        while (head._3 != sample.length+1) {
          //          sample.append(head._1)
          //          var sum = euclideansum_1D(inputData, sample.toArray)
          var sum = 0.0
          (inputData zip lastMinLoss).foreach(f=>{
            val newMinLoss = calculateNormDist_Spatial(f._1, head._1)
            if (newMinLoss < f._2) {
              sum+=newMinLoss
            }
            else sum+=f._2
          })
          var curLoss = sum*1.0/inputData.length
          val marginDecrease = lastLoss - curLoss
          //          marginDecrementQueue.enqueue((head._1, marginDecrease, sample.length))
          marginDecrementQueue.enqueue((head._1, marginDecrease, sample.length+1))
          head = marginDecrementQueue.dequeue()
          //          sample.remove(sample.length-1)
        }
        sample.append(head._1)
        lastLoss -= head._2
        // Decide to add this data, update lastMinLoss
        var sum = 0.0
        for (j <- inputData.indices) {
          val newMinLoss = calculateNormDist_Spatial(inputData(j), head._1)
          if (newMinLoss < lastMinLoss(j)) {
            lastMinLoss(j) = newMinLoss
          }
          sum+=lastMinLoss(j)
        }
//                println(s"finish a greedy round, loss decrease ${head._2}")
        if (lastLoss <= threshold || marginDecrementQueue.isEmpty) {
//                    println(s"quit loss = $lastLoss")
          break
        }
      }
    }
    sample.toArray
  }

  def toSimplePoint(input:String): SimplePoint = {
    val coordinate = input.split(" ")
    new SimplePoint(coordinate(0).toDouble, coordinate(1).toDouble)
  }

  def calSampleSize(count:Long, epsilon: Double, delta:Double): Int = {
    val dominator = 2.0*Math.pow(epsilon, 2)/Math.log(2.0/delta) + 1.0/count
    Math.ceil(1.0/dominator).toInt
  }

  def drawRandomSample[T:ClassTag](inputData:Array[T], samplesize: Int): Array[T] = {
//    val rnd = new Random()
//    val sample:Array[T] = Array.fill[T](samplesize)(inputData(rnd.nextInt(inputData.size)))
//    sample
    Random.shuffle(inputData.toList).take(samplesize).toArray
  }
}
