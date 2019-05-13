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

package com.fuseinfo.jets.util

import java.math.BigInteger
import java.nio.ByteBuffer
import java.sql.{Date, Timestamp}
import java.time.LocalDate

import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import org.apache.avro.generic.{GenericFixed, GenericRecord, GenericRecordBuilder}
import org.apache.avro.util.Utf8

import scala.collection.JavaConversions._

class AvroUtils(obj: AnyRef) {
  def asTimestampMillis(): Timestamp = {
    if (obj != null) new Timestamp(obj.asInstanceOf[java.lang.Long]) else null
  }

  def asTimestampMicros(): Timestamp = {
    if (obj != null) {
      val long = obj.asInstanceOf[java.lang.Long]
      val ts = new Timestamp(long / 1000)
      ts.setNanos(((long % 1000000) * 1000).toInt)
      ts
    } else null
  }

  def asDate(): Date =
    if (obj != null) java.sql.Date.valueOf(LocalDate.ofEpochDay(obj.asInstanceOf[java.lang.Integer].toLong)) else null

  def asDecimal(scale:Int): java.math.BigDecimal = {
    if (obj != null) {
      val buffer = obj match {
        case bb: ByteBuffer => bb.array
        case fixed: GenericFixed => fixed.bytes
      }
      new java.math.BigDecimal(new BigInteger(buffer), scale)
    } else null
  }

  def asStringMap[T](): java.util.Map[String, T] = {
    if (obj != null && obj.isInstanceOf[java.util.Map[_, _]]) {
      obj.asInstanceOf[java.util.Map[CharSequence, AnyRef]].map{p =>
        val pv = p._2
        p._1.toString -> (if (pv != null && pv.isInstanceOf[Utf8]) pv.toString else pv).asInstanceOf[T]}
    } else null
  }

  def asString(): String = {
    if (obj != null) obj.toString else null
  }
}

object AvroUtils{
  val emptySchema: Schema = new Schema.Parser().parse("""{"type":"record","name":"empty","fields":[]}""")

  def createEmpty(schema: Schema): GenericRecord = {
    val builder = new GenericRecordBuilder(schema)
    schema.getFields.foreach{field =>
      val fieldSchema = field.schema
      val fieldName = field.name
      fieldSchema.getType match {
        case Type.RECORD => builder.set(fieldName, createEmpty(fieldSchema))
        case Type.ARRAY => builder.set(fieldName, new java.util.ArrayList[AnyRef])
        case Type.MAP => builder.set(fieldName, new java.util.HashMap[String, AnyRef])
        case Type.UNION => builder.set(fieldName, null)
        case Type.FIXED => builder.set(fieldName, Array.emptyByteArray)
        case Type.STRING => builder.set(fieldName, "")
        case Type.BYTES => builder.set(fieldName, ByteBuffer.allocate(0))
        case Type.INT => builder.set(fieldName, java.lang.Integer.valueOf(0))
        case Type.LONG => builder.set(fieldName, java.lang.Long.valueOf(0))
        case Type.FLOAT => builder.set(fieldName, java.lang.Float.valueOf(0))
        case Type.DOUBLE => builder.set(fieldName, java.lang.Double.valueOf(0))
        case Type.BOOLEAN => builder.set(fieldName, java.lang.Boolean.FALSE)
        case _ => builder.set(fieldName, null)
      }
    }
    builder.build()
  }
}