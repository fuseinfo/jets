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
import com.fuseinfo.jets.kafka.ErrorHandler
import com.fuseinfo.jets.kafka.util.JsonSerde
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.Serde

class JsonParser(paramNode:ObjectNode, schema:Schema, onErrors: List[ErrorHandler[String]])
  extends (Boolean => Serde[GenericRecord]) {
  override def apply(isKey: Boolean): Serde[GenericRecord] = new JsonSerde(schema, onErrors)
}
