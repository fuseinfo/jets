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

package com.fuseinfo.jets.kafka.function

import com.fasterxml.jackson.databind.node.{ObjectNode, ValueNode}
import com.fuseinfo.jets.kafka.{ErrorHandler, KeyMapper}
import com.fuseinfo.jets.kafka.util.{AvroFunctionFactory, JsonUtils}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.slf4j.LoggerFactory

class ScalaKeyMapper(stepName:String, paramNode: ObjectNode, keySchema: Schema, valueSchema: Schema) extends KeyMapper {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private val keyOutSchema = paramNode.get("schema") match {
    case valueNode:ValueNode => (new Schema.Parser).parse(valueNode.asText)
    case objNode: ObjectNode => (new Schema.Parser).parse(objNode.toString)
    case _ => null
  }
  private val keyMapping = paramNode.get("keyMapping").asInstanceOf[ObjectNode]

  private var keyFunc = getFunc(keySchema, valueSchema, keyOutSchema, keyMapping)

  private var counter = 0L

  private val onErrors = JsonUtils.initErrorFuncs(stepName, paramNode.get("onError")) match {
    case Nil => new ErrorHandler {
      override def apply(e: Exception, key: GenericRecord, value: GenericRecord): GenericRecord = {
        logger.error(s"Key:${String.valueOf(key)}\nValue:${String.valueOf(value)}", e)
        null
      }
    } :: Nil
    case list => list
  }

  private def getFunc(keySchema:Schema, valueSchema:Schema, outSchema:Schema, mapping:ObjectNode)= {
    val schemaName = "_schema_" + outSchema.getName.replaceAll("[^a-zA-Z0-9_-]", "")
    AvroFunctionFactory.getFuncClass[(GenericRecord,GenericRecord) => GenericRecord](keySchema,
      valueSchema, outSchema, mapping, 0,
      s"($schemaName:org.apache.avro.Schema) extends ((org.apache.avro.generic.GenericRecord, " +
        "org.apache.avro.generic.GenericRecord) => org.apache.avro.generic.GenericRecord)",
      "(_keyRecord:GenericRecord,_valueRecord:GenericRecord):GenericRecord",
      schemaName).getConstructor(classOf[Schema]).newInstance(outSchema)
  }

  override def apply(key: GenericRecord, value: GenericRecord): GenericRecord = {
    counter += 1
    try {
      keyFunc(key, value)
    } catch {
      case e: Exception =>
        onErrors.foldLeft(null: GenericRecord){(res, errorProcessor) =>
          val output = errorProcessor(e, key, value)
          if (output != null) output else res
        }
    }
  }

  override def getKeySchema: Schema = keyOutSchema

  override def reset(newNode: ObjectNode):Boolean =
    scala.util.Try(keyFunc = new ScalaKeyMapper(stepName, newNode, keySchema, valueSchema).keyFunc).isSuccess

  override def getEventCount: Long = counter

  override def getProcessorSchema: String =
    """{"title": "ScalaKeyMapper","type": "object","properties": {
    "__function":{"type":"string","options":{"hidden":true}},
    "schema":{"type":"string","format":"json","description":"Avro Schema of the key field",
      "options":{"ace":{"useSoftTabs":true,"maxLines":16}}},
    "keyMapping":{"type":"object","additionalProperties":{"format":"scala","options":{"ace":{
      "showLineNumbers":false,"useSoftTabs":true,"maxLines":32}}},"description":"Mapping rule"}
    },"required":["schema","keyMapping"]}"""
}
