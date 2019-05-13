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

import java.net.{InetAddress, URI}
import java.util.UUID

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import com.fuseinfo.jets.kafka.web.DashboardClient.{getGraph, getSchemas, mapper}
import org.java_websocket.client.WebSocketClient
import org.java_websocket.handshake.ServerHandshake
import scala.collection.JavaConversions._

object DashboardClient {
  private val mapper = new ObjectMapper()

  def heartbeatServices(url:String, appName:String, nodes:List[Node], appDef:ObjectNode):Unit = {
    val client = new DashboardClient(appName, nodes, appDef, new URI(url + "/heartbeat"))
    client.connectBlocking()
    while (true) {
      val next = System.currentTimeMillis + 3000
      try {
        val output = getHealthData(nodes, appDef)
        output.put("app", appName)
        output.put("client", client.clientName)
        client.send(output.toString)
      } catch {
        case ex:Exception =>
          //ex.printStackTrace()
          client.reconnectBlocking()
      }
      val diff = next - System.currentTimeMillis
      if (diff > 0) scala.util.Try(Thread.sleep(diff))
    }
  }

  private def getHealthData(nodes:List[Node], appDef:ObjectNode) = {
    val edges = nodes.flatMap(node => node.getOutEdges.map(_._1)).toArray
    val statusNode = mapper.createArrayNode()
    edges.foreach{edge =>
      val edgeNode = mapper.createObjectNode()
      edgeNode.put("name", edge.source.nodeName + ":" + edge.target.nodeName)
      edgeNode.put("total", edge.getEventCount)
      statusNode.add(edgeNode)
    }
    val appNode = mapper.createObjectNode()
    appNode.set("status", statusNode)
    appNode
  }

  private def getGraph(nodes:Array[Node], edges:Array[Edge]): ObjectNode = {
    val nodesNode = mapper.createArrayNode()
    nodes.foreach{node =>
      val dataNode = mapper.createObjectNode()
      dataNode.put("id", node.nodeName)
      val nodeNode = mapper.createObjectNode()
      nodeNode.set("data", dataNode)
      nodesNode.add(nodeNode)
    }
    val edgesNode = mapper.createArrayNode()
    edges.foreach{edge =>
      val source = edge.source.nodeName
      val target = edge.target.nodeName
      val dataNode = mapper.createObjectNode()
      dataNode.put("id", source + ":" + target)
      dataNode.put("source", source)
      dataNode.put("target", target)
      dataNode.put("label", "0\n0")
      val edgeNode = mapper.createObjectNode()
      edgeNode.set("data", dataNode)
      edgesNode.add(edgeNode)
    }
    val graphNode = mapper.createObjectNode()
    graphNode.set("nodes", nodesNode)
    graphNode.set("edges", edgesNode).asInstanceOf[ObjectNode]
  }

  private def getSchemas(nodes:Array[Node]): ArrayNode = {
    val processorsNode = mapper.createArrayNode()
    nodes.foreach{node =>
      val processor = node.processor
      val processorNode = mapper.createObjectNode()
      processorNode.put("name", node.nodeName)
      processorNode.put("schema", processor.getProcessorSchema)
      processorsNode.add(processorNode)
    }
    processorsNode
  }

}

class DashboardClient(appName:String, nodes:List[Node], appDef:ObjectNode, uri:URI)
  extends WebSocketClient(uri) {

  val clientName: String = InetAddress.getLocalHost.getHostName + ":" + UUID.randomUUID()
  private val mapper = new ObjectMapper()
  private val nodeMap = nodes.map(node => node.nodeName -> node).toMap
  private val testedMap = scala.collection.concurrent.TrieMap.empty[String, ObjectNode]

  private val welcome = {
    val appNode = mapper.createObjectNode()
    appNode.put("app", appName)
    appNode.put("client", clientName)
    val edges = nodes.flatMap(node => node.getOutEdges.map(_._1)).toArray
    val nodesArray = nodes.toArray
    val graphNode = getGraph(nodesArray, edges)
    appNode.set("graph", graphNode)
    val schemasNode = getSchemas(nodesArray)
    appNode.set("schemas", schemasNode)
    appNode.set("def", appDef)
    appNode.toString
  }

  override def onOpen(handshakedata: ServerHandshake): Unit = {
    send(welcome)
  }

  override def onMessage(message: String): Unit = {
    val rootNode = mapper.readTree(message)
    val pendingNodes = rootNode.get("pending")
    pendingNodes.fields().foreach{nodeEntry =>
      val evalNode = mapper.createArrayNode()
      val nodeName = nodeEntry.getKey
      nodeMap.get(nodeName) match {
        case Some(node) =>
          val processor = node.processor
          val jsonNode = nodeEntry.getValue.asInstanceOf[ObjectNode]
          val lastRun = testedMap.get(nodeName).map(_.equals(jsonNode))
          if (!lastRun.getOrElse(false)) {
            testedMap.put(nodeName, jsonNode)
            val evalObj = mapper.createObjectNode()
            evalObj.put("node", nodeName)
            evalObj.put("isSuccess", processor.reset(jsonNode))
            evalNode.add(evalObj)
          }
        case None =>
          val evalObj = mapper.createObjectNode()
          evalObj.put("node", nodeName)
          evalObj.put("isSuccess", false)
          evalNode.add(evalObj)
      }
      if (evalNode.size() > 0) {
        val rootNode = mapper.createObjectNode()
        rootNode.put("app", appName)
        rootNode.put("client", clientName)
        rootNode.set("eval", evalNode)
        send(rootNode.toString)
      }
    }

  }

  override def onClose(code: Int, reason: String, remote: Boolean): Unit = {}

  override def onError(ex: Exception): Unit = {
    ex.printStackTrace()
  }
}