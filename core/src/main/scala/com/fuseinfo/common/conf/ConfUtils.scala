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

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import com.fuseinfo.jets.util.VarUtils

import scala.collection.JavaConversions._

object ConfUtils {

  def jsonToMap(objNode: ObjectNode): java.util.Map[String, AnyRef] = {
    val result = new java.util.LinkedHashMap[String, AnyRef]
    objNode.fields().foreach{field =>
      val fieldName = field.getKey
      result.put(fieldName, jsonToObj(field.getValue))
    }
    result
  }

  def jsonToObj(jsonNode: JsonNode): AnyRef = jsonNode match {
    case objNode:ObjectNode => jsonToMap(objNode)
    case arrNode:ArrayNode => arrNode.elements().map(jsonToObj).toArray
    case _ => jsonNode.asText
  }

  def jsonToMap(objNode: ObjectNode, vars:java.util.Map[String, String]): java.util.Map[String, AnyRef] = {
    val result = new java.util.LinkedHashMap[String, AnyRef]
    objNode.fields().foreach{field =>
      val fieldName = field.getKey
      result.put(fieldName, jsonToObj(field.getValue, vars))
    }
    result
  }

  def jsonToObj(jsonNode: JsonNode, vars:java.util.Map[String, String]): AnyRef = jsonNode match {
    case objNode:ObjectNode => jsonToMap(objNode)
    case arrNode:ArrayNode => arrNode.elements().map(jsonToObj).toArray
    case _ => VarUtils.enrichString(jsonNode.asText, vars)
  }
}
