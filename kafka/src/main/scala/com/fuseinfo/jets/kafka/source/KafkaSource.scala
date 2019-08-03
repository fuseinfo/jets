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

package com.fuseinfo.jets.kafka.source

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode, ValueNode}
import com.fuseinfo.jets.kafka.{KafkaFlowBuilder, SourceStream}
import com.fuseinfo.jets.util.VarUtils
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.{Consumed, KStream, Predicate}

import scala.collection.JavaConversions._

class KafkaSource(stepName: String, paramNode:ObjectNode) extends SourceStream {
  @transient private val parser = new Schema.Parser
  private val keySchema:Schema = getSchema(paramNode.get("keySchema"))
  private val valueSchema:Schema = getSchema(paramNode.get("valueSchema"))

  private val keyParser:Serde[GenericRecord] = getParser(true)
  private val valueParser:Serde[GenericRecord] = getParser(false)

  private var counter = 0L

  private val topics = paramNode.get("topic") match {
    case array:ArrayNode =>
      val list = new java.util.ArrayList[String](array.size)
      array.elements().foreach(nd => list.add(VarUtils.enrichString(nd.asText, KafkaFlowBuilder.vars)))
      list
    case nd =>
      java.util.Arrays.asList(VarUtils.enrichString(nd.asText, KafkaFlowBuilder.vars).split(",").map(_.trim):_*)
  }

  private def getParser(isKey:Boolean): Serde[GenericRecord] = {
    val schema = if (isKey) keySchema else valueSchema
    (paramNode.get(if (isKey) "keyParser" else "valueParser") match {
      case null => paramNode.get("parser")
      case nd => nd
    }) match {
      case null => new GenericAvroSerde
      case objNode:ObjectNode =>
        val newNode = objNode.deepCopy
        newNode.without("__parser")
        createParser(objNode.get("__parser").asText, newNode, schema, isKey)
      case jsonNode => createParser(jsonNode.asText, paramNode.objectNode, schema, isKey)
    }
  }

  private def getSchema(jsonNode: JsonNode):Schema = {
    if (jsonNode != null) parser.parse(jsonNodeToString(jsonNode)) else null
  }

  private def createParser(clazz:String, objNode:ObjectNode, schema:Schema, isKey:Boolean): Serde[GenericRecord] = {
    (try {
      Class.forName(clazz)
    } catch {
      case _:ClassNotFoundException => Class.forName(KafkaFlowBuilder.packagePrefix + "parser." + clazz)
    }).getDeclaredConstructor(classOf[ObjectNode], classOf[Schema])
      .newInstance(objNode, schema)
      .asInstanceOf[Boolean => Serde[GenericRecord]](isKey)
  }

  private def jsonNodeToString(jsonNode:JsonNode): String = jsonNode match {
    case valueNode:ValueNode => valueNode.asText
    case _ => jsonNode.toString
  }

  override def apply(builder: StreamsBuilder): KStream[GenericRecord, GenericRecord] =
    builder.stream(topics, Consumed.`with`(keyParser, valueParser))
      .filter(new Predicate[GenericRecord, GenericRecord](){
        override def test(key: GenericRecord, value: GenericRecord): Boolean = {
          counter += 1
          true
        }
      })

  override def getEventCount:Long = counter

  override def getKeySchema: Schema = keySchema

  override def getValueSchema: Schema = valueSchema

  override def getProcessorSchema: String =
    """{"title": "KafkaSource","type": "object","properties": {
    "__source":{"type":"string","options":{"hidden":true}},
    "keySchema":{"type":"string","format":"json","description":"Avro Schema of the key field",
      "options":{"ace":{"useSoftTabs":true,"maxLines":16}}},
    "valueSchema":{"type":"string","format":"json","description":"Avro Schema of the value field",
      "options":{"ace":{"useSoftTabs":true,"maxLines":16}}},
    "keyParser":{"type":"object","description":"Parser for the key field","properties": {
      "__parser":{"type":"string","description":"parser class"}}},
    "valueParser":{"type":"object","description":"Parser for the value field","properties": {
      "__parser":{"type":"string","description":"parser class"}}},
    "parser":{"type":"object","description":"Parser for both key and value fields","properties": {
      "__parser":{"type":"string","description":"timer class"}}}
     },"required":["valueSchema"]}"""
}
