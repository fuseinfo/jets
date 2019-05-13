/*
 * Copyright (c) 2019 Fuseinfo Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.fuseinfo.jets.kafka.web

import com.fuseinfo.jets.kafka.StreamProcessor

class Node(val nodeName:String, val processor:StreamProcessor) {
  private val inEdges = collection.mutable.ArrayBuffer.empty[Edge]
  val outEdges = collection.mutable.Map.empty[Edge, Int]
  def getInEdges: Iterator[Edge] = inEdges.iterator

  def setInEdges(edges: Edge*): Unit = {
    inEdges.clear()
    inEdges ++ edges
  }

  def addInEdge(edge: Edge): Unit = inEdges += edge

  def getOutEdges: Iterator[(Edge, Int)] = outEdges.iterator

  def addOutEdge(edge: Edge, idx:Int):Unit = outEdges.put(edge, idx)

  def getEventCount(edge:Edge):Long = processor.getEventCount(outEdges(edge))
}

class Edge(val edgeName: String, val source:Node, val target:Node) {
  def getEventCount: Long = source.getEventCount(this)
}

