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

package com.fuseinfo.jets.kafka.parser

import com.fasterxml.jackson.databind.node.ObjectNode
import com.fuseinfo.jets.kafka.util.SchemaRegistrySerde
import com.fuseinfo.jets.kafka.{ErrorHandler, KafkaFlowBuilder}
import com.fuseinfo.jets.util.VarUtils
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.Serde

import scala.collection.JavaConversions._

class SchemaRegistryParser(paramNode:ObjectNode, schema:Schema, onErrors: List[ErrorHandler[String]]) extends (Boolean => Serde[GenericRecord]) {
  private val keys = Set("schema.registry.url", "basic.auth.credentials.source", "schema.registry.basic.auth.user.info",
    "key.subject.name.strategy", "value.subject.name.strategy", "auto.register.schemas", "max.schemas.per.subject")

  override def apply(isKey: Boolean): Serde[GenericRecord] = {
    val serde = new SchemaRegistrySerde(onErrors)
    val serdeConfig = new java.util.HashMap[String, String]
    paramNode.fields().foreach{entry =>
      val key = entry.getKey
      if (keys.contains(key)) serdeConfig.put(key, VarUtils.enrichString(entry.getValue.asText, KafkaFlowBuilder.vars))
    }
    serde.configure(serdeConfig, isKey)
    serde
  }
}
