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

package com.fuseinfo.jets.beam.parser

import com.fuseinfo.jets.beam.BeamParser
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.generic.GenericRecord

class SchemaRegistryParser(paramNode:java.util.Map[String, AnyRef], schema:String, isKey:java.lang.Boolean)
  extends BeamParser[Array[Byte]] {

  private val topic = paramNode.get("topic").toString
  private val url = paramNode.get("schema.registry.url").toString

  @transient private lazy val client = new CachedSchemaRegistryClient(url, 1000)
  @transient private lazy val deserializer = {
    val avroDeserializer = new KafkaAvroDeserializer(client)
    avroDeserializer.configure(paramNode, isKey)
    avroDeserializer
  }

  override def apply(bytes: Array[Byte]): GenericRecord =
    deserializer.deserialize(topic, bytes).asInstanceOf[GenericRecord]
}
