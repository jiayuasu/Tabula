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
package org.datasyslab.samplingcube.algorithms

object FindCombinations {

  /** arr[]  ---> Input Array
      data[] ---> Temporary array to store current combination
      start & end ---> Staring and Ending indexes in arr[]
      index  ---> Current index in data[]
      r ---> Size of a combination to be printed *//* arr[]  ---> Input Array
      data[] ---> Temporary array to store current combination
      start & end ---> Staring and Ending indexes in arr[]
      index  ---> Current index in data[]
      r ---> Size of a combination to be printed */
  def combinationUtil(arr: Seq[String], data: Array[String], start: Int, end: Int, index: Int, r: Int): Seq[String] = { // Current combination is ready to be printed, print it
    if (index == r) {
      // Merge all combinations to a single string
      return Seq(data.mkString(","))
    }
    // replace index with all possible elements. The condition
    // "end-i+1 >= r-index" makes sure that including one element
    // at index will make a combination with remaining elements
    // at remaining positions
    var i = start
    var result:Seq[String] = Seq()
    while ( {
      i <= end && (end - i + 1) >= r - index
    }) {
      data(index) = arr(i)
      result = result ++ combinationUtil(arr, data, i + 1, end, index + 1, r)
      i += 1
    }
    return result
  }

  // The main function that prints all combinations of size r
  // in arr[] of size n. This function mainly uses combinationUtil()
  def find(arr: Seq[String], n: Int, r: Int): Seq[String] = { // A temporary array to store all combination one by one
    val data = new Array[String](r)
    // Print all combination using temprary array 'data[]'
    return combinationUtil(arr, data, 0, n - 1, 0, r)
  }
}
