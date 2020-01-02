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
package org.datasyslab.samplingcube.algorithms

import org.apache.spark.sql.Row
import org.datasyslab.samplingcube.utils.CommonFunctions

import scala.collection.mutable

object MinimumDominatingSet extends CommonFunctions {

  def GreedyAlgorithm(edges: Array[Row]): mutable.Iterable[Row] = {
    logger.info(cubeLogPrefix + s"[Sample selection stage] Build the SamGraph for Representative Sample Selection")
    var uncoveredNodes = new mutable.HashMap[Long, Node]()
    edges.foreach(f => {
      try {
        uncoveredNodes += (f.getString(0).toLong -> new Node(f.getString(0).toLong, mutable.HashSet(f.getString(1).split(",").map(g => g.toLong).toList: _*)))
      }
      catch {
        case e:java.lang.ClassCastException =>{
          uncoveredNodes += (f.getLong(0) -> new Node(f.getLong(0), mutable.HashSet(f.getString(1).split(",").map(g => g.toLong).toList: _*)))
        }
      }
    })
    // keep track of number of nodes and number of uncovered nodes
    var allNodesAreCovered: Boolean = false
    // keep track of nodes that have been selected
    var visitedNodes = mutable.ArrayBuffer.empty[Long]
    // keep track of edges in a tail head format, inverse format
    var visitedEdges = new mutable.HashMap[Long, Long]()

    util.control.Breaks.breakable {
      while (uncoveredNodes.size != 0) {
        var maxCoverNum = 0
        var maxNode = uncoveredNodes.maxBy(_._2.uncoveredOutgoingEdges.size)

        // Remove all covered node from uncoveredNodes
        maxNode._2.uncoveredOutgoingEdges.foreach(tailNode => {
          // Note that, we assume each node itself has a self-cycle
          uncoveredNodes.remove(tailNode)

          // Check in the inverse mapping. Use the hashmap to avoid duplicate keys
          visitedEdges += (tailNode -> maxNode._1)
        })
        // Record the selected nodes
        visitedNodes += maxNode._1

        logger.info(cubeLogPrefix + s"[Sample selection stage] Select Node ${maxNode._1} outdegree ${maxNode._2.uncoveredOutgoingEdges.size}")
        // Update the uncovered outgoing edges in uncoveredNodes
        uncoveredNodes = uncoveredNodes.map(keyNode => {
          var cursorNode = keyNode._2
          var edges = cursorNode.uncoveredOutgoingEdges
          logger.debug(cubeLogPrefix + "[Sample selection stage] Before updating the outdegree:" + edges.size)

          maxNode._2.uncoveredOutgoingEdges.foreach(coveredTail => {
            edges.remove(coveredTail)
          })
          cursorNode = new Node(cursorNode.id, edges)
          logger.debug(cubeLogPrefix + "[Sample selection stage] After updating the outdegree:" + edges.size)
          (keyNode._1 -> cursorNode)
        })

        // Remove self representated nodes
        if (maxNode._2.uncoveredOutgoingEdges.size == 0) {
          visitedEdges += (maxNode._1 -> maxNode._1)
          uncoveredNodes.remove(maxNode._1)
        }
      }
    }
    logger.info(cubeLogPrefix + s"[Sample selection stage] ${visitedNodes.size} iceberg cells are selected to be materialized")
    // Return all final representations
    visitedEdges.map(f => Row(f._1, f._2))
  }
}

class Node(var id: Long, var uncoveredOutgoingEdges: mutable.HashSet[Long]) {
}