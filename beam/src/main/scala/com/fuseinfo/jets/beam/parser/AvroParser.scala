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

import com.fuseinfo.common.io.ByteBufferInputStream
import com.fuseinfo.jets.beam.BeamParser
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory

class AvroParser(paramNode:java.util.Map[String, AnyRef], schema:String, isKey:java.lang.Boolean)
  extends BeamParser[Array[Byte]]{

  @transient private lazy val stream = new ByteBufferInputStream
  @transient private lazy val decoder = DecoderFactory.get.binaryDecoder(stream, null)
  @transient private lazy val reader = if (schema != null) getReader(new Schema.Parser().parse(schema)) else null

  private def getReader(schema: Schema) = new GenericDatumReader[GenericRecord](schema)

  override def apply(data: Array[Byte]): GenericRecord = {
    if (data == null || data.isEmpty) {
      null
    } else {
      stream.setData(data)
      reader.read(null, decoder)
    }
  }
}
