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

import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

object NullSerde extends Serde[GenericRecord]  {
  override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}

  override def serializer(): Serializer[GenericRecord] = new NullSerializer

  override def deserializer(): Deserializer[GenericRecord] = new NullDeserializer

  class NullSerializer extends Serializer[GenericRecord] {
    override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = {}

    override def serialize(topic: String, data: GenericRecord): Array[Byte] = null

    override def close(): Unit = {}
  }

  class NullDeserializer extends Deserializer[GenericRecord] {
    override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = {}

    override def deserialize(topic: String, data: Array[Byte]): GenericRecord = null

    override def close(): Unit = {}
  }
}

