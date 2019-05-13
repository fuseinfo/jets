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

package com.fuseinfo.jets.beam.formatter

import java.io.ByteArrayOutputStream

import com.fuseinfo.jets.beam.BeamFormatter
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.avro.io.EncoderFactory

class AvroFormatter(paramNode:java.util.Map[String, AnyRef], schema:String, isKey:java.lang.Boolean)
  extends BeamFormatter[Array[Byte]] {

  @transient private lazy val stream = new ByteArrayOutputStream
  @transient private lazy val encoder = EncoderFactory.get.binaryEncoder(stream, null)
  @transient private lazy val writer = if (schema != null) getWriter(new Schema.Parser().parse(schema)) else null

  override def apply(row: GenericRecord): Array[Byte] = {
    if (row != null & writer != null) {
      stream.reset()
      writer.write(row, encoder)
      encoder.flush()
      stream.toByteArray
    } else null
  }

  private def getWriter(schema: Schema) = new GenericDatumWriter[GenericRecord](schema)

}