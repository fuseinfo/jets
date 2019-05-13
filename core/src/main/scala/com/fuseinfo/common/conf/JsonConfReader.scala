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

package com.fuseinfo.common.conf

import java.io.{File, FileInputStream, InputStream}
import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.databind.node.{NullNode, ObjectNode, TextNode}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClientBuilder
import scala.collection.JavaConversions._

class JsonConfReader(val jf:JsonFactory = new JsonFactory()) {
  @transient private val mapper = new ObjectMapper(jf)

  def readFromStream(is:InputStream, expanded:Boolean = false): JsonNode = mapper.readTree(is) match {
    case objNode:ObjectNode if expanded => expandObject(objNode)
    case node:JsonNode => node
  }

  def readFromString(str:String): JsonNode = {
    mapper.readTree(str)
  }

  def expandObject(obj: ObjectNode): ObjectNode = {
    obj.fields().foreach{entry =>
      val node = entry.getValue
      node match {
        case objNode: ObjectNode if isRefNode(objNode) =>
          obj.replace(entry.getKey, expandRef(node.asInstanceOf[ObjectNode]))
        case objNode: ObjectNode => expandObject(objNode)
        case textNode: TextNode =>
          val text = textNode.asText
          if (text.contains("\n")) {
            val trimed = text.replace("\r\n","\n")
              .replace("\t","  ")
              .replaceAll("\\s+\n", "\n")
              .replaceAll("\\s+$", "")
            obj.replace(entry.getKey, new TextNode(trimed))
          }
        case _ =>
      }
    }
    obj
  }

  private def isRefNode(node: JsonNode): Boolean = node match {
    case objNode: ObjectNode if objNode.get("$ref") != null => true
    case _ => false
  }

  private def expandRef(node: ObjectNode): JsonNode = {
    val ref = node.get("$ref").asText()
    val refLower = ref.toLowerCase
    val is = if (refLower.startsWith("http://") || refLower.startsWith("https://")) {
      val httpClient = HttpClientBuilder.create().build()
      val request = new HttpGet(ref)
      val response = httpClient.execute(request)
      response.getEntity.getContent
    } else {
      val refFile = new File(ref)
      if (scala.util.Try(refFile.canRead).getOrElse(false)) new FileInputStream(refFile)
      else getClass.getClassLoader.getResourceAsStream(ref)
    }
    if (is != null) {
      val nd = if (ref.toLowerCase.contains(".yaml")) {
        readFromStream(is, true)
      } else {
        val str = scala.io.Source.fromInputStream(is).getLines().mkString("\n")
        new TextNode(str)
      }
      is.close()
      nd
    } else NullNode.getInstance
  }

}