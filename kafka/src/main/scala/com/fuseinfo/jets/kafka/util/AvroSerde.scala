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

package com.fuseinfo.jets.kafka.util

import java.io.ByteArrayOutputStream

import com.fuseinfo.common.io.ByteBufferInputStream
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

class AvroSerde(var schema: Schema = null) extends Serde[GenericRecord]{
  def this(str: String) {
    this((new Schema.Parser).parse(str))
  }

  override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = {
    configs.get("schema") match {
      case s: Schema => schema = s
      case str: String => (new Schema.Parser).parse(str)
    }
  }

  override def close(): Unit = { }

  override def serializer(): Serializer[GenericRecord] = new AvroSerializer(schema)

  override def deserializer(): Deserializer[GenericRecord] = new AvroDeserializer(schema)
}

class AvroSerializer(var schema: Schema = null) extends Serializer[GenericRecord] {
  private val stream = new ByteArrayOutputStream
  private val encoder = EncoderFactory.get.binaryEncoder(stream, null)
  private var writer = if (schema != null) getWriter(schema) else null

  private def getWriter(schema: Schema) = new GenericDatumWriter[GenericRecord](schema)

  override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = {
    configs.get("schema") match {
      case s: Schema =>
        schema = s
        writer = getWriter(schema)
      case str: String =>
        schema = (new Schema.Parser).parse(str)
        writer = getWriter(schema)
      case _ =>
    }
  }

  override def serialize(topic: String, data: GenericRecord): Array[Byte] = this.synchronized {
    if (data != null) {
      stream.reset()
      writer.write(data, encoder)
      encoder.flush()
      stream.toByteArray
    } else null//Array.emptyByteArray
  }

  override def close(): Unit = {}
}

class AvroDeserializer(var schema: Schema = null) extends  Deserializer[GenericRecord] {
  private val stream = new ByteBufferInputStream
  private val decoder = DecoderFactory.get.binaryDecoder(stream, null)
  private var reader = if (schema != null) getReader(schema) else null

  private def getReader(schema: Schema) = new GenericDatumReader[GenericRecord](schema)

  override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = {
    configs.get("schema") match {
      case s: Schema =>
        schema = s
        reader = getReader(schema)
      case str: String =>
        schema = (new Schema.Parser).parse(str)
        reader = getReader(schema)
      case _ =>
    }
  }

  override def deserialize(topic: String, data: Array[Byte]): GenericRecord = this.synchronized{
    if (data == null || data.isEmpty) {
      null
    } else {
      stream.setData(data)
      reader.read(null, decoder)
    }
  }

  override def close(): Unit = {}
}
