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

package com.fuseinfo.jets.kafka.sink

import com.fasterxml.jackson.databind.node.ObjectNode
import com.fuseinfo.jets.kafka.{KafkaFlowBuilder, StreamSink}
import com.fuseinfo.jets.kafka.util.NullSerde
import com.fuseinfo.jets.util.VarUtils
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.{KStream, Produced}

class KafkaSink(paramNode:ObjectNode, keySchema:Schema, valueSchema:Schema) extends StreamSink {
  private val topic = VarUtils.enrichString(paramNode.get("topic").asText, KafkaFlowBuilder.vars)
  private val keyFormatter = getFormatter(true)
  private val valueFormatter = getFormatter(false)

  override def apply(input: KStream[GenericRecord, GenericRecord]): Unit =
    input.to(topic, Produced.`with`(keyFormatter, valueFormatter))

  private def getFormatter(isKey:Boolean): Serde[GenericRecord] = {
    val schema = if (isKey) keySchema else valueSchema
    (paramNode.get(if (isKey) "keyFormatter" else "valueFormatter") match {
      case null => paramNode.get("formatter")
      case nd => nd
    }) match {
      case null => NullSerde
      case objNode:ObjectNode =>
        val newNode = objNode.deepCopy
        newNode.without("__formatter")
        createFormatter(objNode.get("__formatter").asText, newNode, schema, isKey)
      case jsonNode => createFormatter(jsonNode.asText, paramNode.objectNode, schema, isKey)
    }
  }

  private def createFormatter(clazz:String, objNode:ObjectNode, schema:Schema, isKey:Boolean): Serde[GenericRecord] = {
    (try {
      Class.forName(clazz)
    } catch {
      case _:ClassNotFoundException => Class.forName(KafkaFlowBuilder.packagePrefix + "formatter." + clazz)
    }).getDeclaredConstructor(classOf[ObjectNode], classOf[Schema])
      .newInstance(objNode, schema)
      .asInstanceOf[Boolean => Serde[GenericRecord]](isKey)
  }

  override def getProcessorSchema: String =
    """{"title": "KafkaSink","type": "object","properties": {
    "__sink":{"type":"string","options":{"hidden":true}},
    "keyFormatter":{"type":"object","description":"Formatter for the key field","properties": {
      "__parser":{"type":"string","description":"formatter class"}}},
    "valueFormatter":{"type":"object","description":"Formatter for the value field","properties": {
      "__parser":{"type":"string","description":"formatter class"}}},
    "formatter":{"type":"object","description":"Formatter for both key and value fields","properties": {
      "__parser":{"type":"string","description":"timer class"}}}
     }}"""
}
