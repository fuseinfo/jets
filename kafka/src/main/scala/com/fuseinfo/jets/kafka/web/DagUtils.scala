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

import scala.collection.mutable

object DagUtils {

  def dfs(nodes: Iterable[Node]):mutable.MutableList[Node] = {
    val ll = mutable.MutableList.empty[Node]
    val nodeMap = nodes.zipWithIndex.map(p => p._1.nodeName -> p._2).toMap
    val pMarks = new Array[Boolean](nodeMap.size)
    val tMarks = new Array[Boolean](nodeMap.size)
    nodes.foreach(node =>
      visit(node, nodeMap, pMarks, tMarks, ll)
    )
    ll
  }

  def visit(node:Node, nodeMap:Map[String, Int], pMarks:Array[Boolean], tMarks:Array[Boolean],
            ll:mutable.MutableList[Node]): Unit = {
    val idx = nodeMap(node.nodeName)
    if (!pMarks(idx)) {
      if (tMarks(idx)) throw new Exception("not a DAG")
      tMarks(idx) = true
      node.outEdges.foreach {case (edge, _) =>
        visit(edge.target, nodeMap, pMarks, tMarks, ll)
      }
      pMarks(idx) = true
      node +=: ll
    }
  }
}
