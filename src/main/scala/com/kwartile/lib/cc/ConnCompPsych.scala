/**
  * Copyright (c) 2017 Kwartile, Inc., http://www.kwartile.com
  * Permission is hereby granted, free of charge, to any person obtaining a copy
  * of this software and associated documentation files (the "Software"), to deal
  * in the Software without restriction, including without limitation the rights
  * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
  * copies of the Software, and to permit persons to whom the Software is
  * furnished to do so, subject to the following conditions:
  *
  * The above copyright notice and this permission notice shall be included in all
  * copies or substantial portions of the Software.
  *
  * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
  * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
  * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
  * SOFTWARE.
  */

/**
  * Map-reduce implementation of Connected Component
  * Given lists of subgraphs, returns all the nodes that are connected.
  */

package com.kwartile.lib.cc

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.annotation.tailrec
import scala.collection.mutable

object ConnCompPsych extends Serializable {

  /**
    * Applies Small Star operation on RDD of nodePairs
    *
    * @param nodePairs on which to apply Small Star operations
    * @return new nodePairs after the operation and conncectivy change count
    */
  def smallStar(nodePairs: RDD[(Long, Long)]): (RDD[(Long, Long)], Long) = {

    /**
      * generate RDD of (self, List(neighbors)) where self > neighbors
      * E.g.: nodePairs (1, 4), (6, 1), (3, 2), (6, 5)
      * will result into (4, List(1)), (6, List(1)), (3, List(2)), (6, List(5))
      */
    /**
      * reduce on self to get list of all its neighbors.
      * E.g: (4, List(1)), (6, List(1)), (3, List(2)), (6, List(5))
      * will result into (4, List(1)), (6, List(1, 5)), (3, List(2))
      * Note:
      * (1) you may need to tweak number of partitions.
      * (2) also, watch out for data skew. In that case, consider using rangePartitioner
      */
    val sortedPairs = nodePairs.
      map { x =>
        val (self, neighbor) = (x._1, x._2)
        if (self > neighbor) (self, neighbor) else (neighbor, self)
      }
    val allNeighbours = sortedPairs.map(x => (x._1, Set(x._2))).
      reduceByKey(_ union _)

    /**
      * Apply Small Star operation on (self, List(neighbor)) to get newNodePairs and count the change in connectivity
      */

    val uniqueNewNodePairs = allNeighbours.flatMap { x =>
      val self = x._1
      val neighbors = x._2
      val minVertex = argMin(neighbors + self)
      (neighbors + self).
        map(v => (v, minVertex)).filter { case (v, minV) => (v <= self && v != minV) || (self == v) }
    }.distinct()

    val connChange = sortedPairs.subtract(uniqueNewNodePairs).count
    println("Psych SmallStar pairs:")
    uniqueNewNodePairs.collect.foreach(x => println(s"\t $x"))
    println(s"ConnChange: $connChange")
    (uniqueNewNodePairs, connChange)
  }

  /**
    * Apply Large Star operation on a RDD of nodePairs
    *
    * @param nodePairs on which to apply Large Star operations
    * @return new nodePairs after the operation and conncectivy change count
    */
  def largeStar(nodePairs: RDD[(Long, Long)]): (RDD[(Long, Long)], Long) = {

    /**
      * generate RDD of (self, List(neighbors))
      * E.g.: nodePairs (1, 4), (6, 1), (3, 2), (6, 5)
      * will result into (4, List(1)), (1, List(4)), (6, List(1)), (1, List(6)), (3, List(2)), (2, List(3)), (6, List(5)), (5, List(6))
      */

    val pairs = nodePairs.flatMap(x => {
      val (self, neighbor) = (x._1, x._2)
      if (self == neighbor)
        List((self, neighbor))
      else
        List((self, neighbor), (neighbor, self))
    })

    /**
      * reduce on self to get list of all its neighbors.
      * We are using aggregateByKey. You may choose to use reduceByKey as in the Small Star
      * E.g: (4, List(1)), (1, List(4)), (6, List(1)), (1, List(6)), (3, List(2)), (2, List(3)), (6, List(5)), (5, List(6))
      * will result into (4, List(1)), (1, List(4, 6)), (6, List(1, 5)), (3, List(2)), (2, List(3)), (5, List(6))
      * Note:
      * (1) you may need to tweak number of partitions.
      * (2) also, watch out for data skew. In that case, consider using rangePartitioner
      */

    val localAdd = (s: mutable.HashSet[Long], v: Long) => s += v
    val partitionAdd = (s1: mutable.HashSet[Long], s2: mutable.HashSet[Long]) => s1 ++= s2
    val allNeighbors = pairs.aggregateByKey(mutable.HashSet.empty[Long] /*, rangePartitioner*/)(localAdd, partitionAdd)

    /**
      * Apply Large Star operation on (self, List(neighbor)) to get newNodePairs and count the change in connectivity
      */

    val newNodePairs = allNeighbors.flatMap { case (self, neighbors) =>
      val minVertex = argMin(neighbors.toSet + self)
      val uniqueNewNodePairs = (neighbors + self).map(neighbor => {
        (neighbor, minVertex)
      }).filter(x => {
        val neighbor = x._1
        val minVertex = x._2
        (neighbor > self && neighbor != minVertex) || (neighbor == self)
      }).toSet
      uniqueNewNodePairs
    }.persist(StorageLevel.MEMORY_AND_DISK_SER)

    //    val connectivtyChangeCount = (uniqueNewNodePairs diff neighbors.map((self, _))).size
    //    (uniqueNewNodePairs, connectivtyChangeCount)

    val connsChange = pairs.filter(x => x._1 >= x._2).subtract(newNodePairs).count

    /**
      * Sum all the changeCounts
      */
    (newNodePairs, connsChange)
  }

  private def argMin(nodes: Set[Long]): Long = {
    nodes.min(Ordering.by((node: Long) => node))
  }

  /**
    * Build nodePairs given a list of nodes.  A list of nodes represents a subgraph.
    *
    * @param nodes that are part of a subgraph
    * @return nodePairs for a subgraph
    */
  private def buildPairs(nodes: List[Long]): List[(Long, Long)] = {
    buildPairs(nodes.head, nodes.tail, null.asInstanceOf[List[(Long, Long)]])
  }

  @tailrec
  private def buildPairs(node: Long, neighbors: List[Long], partialPairs: List[(Long, Long)]): List[(Long, Long)] = {
    if (neighbors.isEmpty) {
      if (partialPairs != null)
        List((node, node)) ::: partialPairs
      else
        List((node, node))
    } else if (neighbors.length == 1) {
      val neighbor = neighbors.head
      if (node > neighbor)
        if (partialPairs != null) List((node, neighbor)) ::: partialPairs else List((node, neighbor))
      else if (partialPairs != null) List((neighbor, node)) ::: partialPairs else List((neighbor, node))
    } else {
      val newPartialPairs = neighbors.flatMap(neighbor => {
        if (node > neighbor)
          List((node, neighbor))
        else
          List((neighbor, node))
      })

      if (partialPairs != null)
        buildPairs(neighbors.head, neighbors.tail, newPartialPairs ::: partialPairs)
      else
        buildPairs(neighbors.head, neighbors.tail, newPartialPairs)
    }
  }

  /**
    * Implements alternatingAlgo.  Converges when the changeCount is either 0 or does not change from the previous iteration
    *
    * @param nodePairs                        for a graph
    * @param largeStarConnectivityChangeCount change count that resulted from the previous iteration
    * @param smallStarConnectivityChangeCount change count that resulted from the previous iteration
    * @param didConverge                      flag to indicate the alorigth converged
    * @param currIterationCount               counter to capture number of iterations
    * @param maxIterationCount                maximum number iterations to try before giving up
    * @return RDD of nodePairs
    */

  @tailrec
  private def alternatingAlgo(
                               nodePairs: RDD[(Long, Long)],
                               largeStarConnectivityChangeCount: Long,
                               smallStarConnectivityChangeCount: Long,
                               didConverge: Boolean,
                               currIterationCount: Int,
                               maxIterationCount: Int
                             ): (RDD[(Long, Long)], Boolean, Int) = {

    val iterationCount = currIterationCount + 1
    if (didConverge)
      (nodePairs, true, currIterationCount)
    else if (currIterationCount >= maxIterationCount) {
      (nodePairs, false, currIterationCount)
    }
    else {

      val (nodePairsLargeStar, currLargeStarConnectivityChangeCount) = largeStar(nodePairs)

      val (nodePairsSmallStar, currSmallStarConnectivityChangeCount) = smallStar(nodePairsLargeStar)

      if ((currLargeStarConnectivityChangeCount == largeStarConnectivityChangeCount &&
        currSmallStarConnectivityChangeCount == smallStarConnectivityChangeCount) ||
        (currSmallStarConnectivityChangeCount == 0 && currLargeStarConnectivityChangeCount == 0)) {
        alternatingAlgo(nodePairsSmallStar, currLargeStarConnectivityChangeCount,
          currSmallStarConnectivityChangeCount, didConverge = true, iterationCount, maxIterationCount)
      }
      else {
        alternatingAlgo(nodePairsSmallStar, currLargeStarConnectivityChangeCount,
          currSmallStarConnectivityChangeCount, didConverge = false, iterationCount, maxIterationCount)
      }
    }
  }

  /**
    * Driver function
    *
    * @param cliques           list of nodes representing subgraphs (or cliques)
    * @param maxIterationCount maximum number iterations to try before giving up
    * @return Connected Components as nodePairs where second member of the nodePair is the minimum node in the component
    */
  def run(cliques: RDD[List[Long]], maxIterationCount: Int): (RDD[(Long, Long)], Boolean, Int) = {

    val nodePairs: RDD[(Long, Long)] = cliques.map(clique => buildPairs(clique)).flatMap(identity)

    val (cc, ifConverge, iterCount) = alternatingAlgo(nodePairs, 9999999, 9999999, didConverge = false, 0, maxIterationCount)

    if (ifConverge) {
      (cc, ifConverge, iterCount)
    } else {
      (null.asInstanceOf[RDD[(Long, Long)]], ifConverge, iterCount)
    }
  }
}


