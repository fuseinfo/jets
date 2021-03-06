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
import com.fuseinfo.jets.util.AvroUtils
import org.apache.avro.Schema
import org.apache.avro.Schema.{Field, Type}
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.json4s._
import org.json4s.jackson.JsonMethods

class JsonParser(paramNode:java.util.Map[String, AnyRef], schema:String, isKey:java.lang.Boolean)
    extends BeamParser[Array[Byte]]{

  @transient private lazy val schemaIn = new Schema.Parser().parse(schema)

  override def apply(bytes: Array[Byte]): GenericRecord = {
    if (bytes != null && bytes.nonEmpty) {
      getValue(schemaIn, JsonMethods.parse(new String(bytes, "UTF-8"))).asInstanceOf[GenericRecord]
    } else null
  }

  private def getValue(schema:Schema, v:JValue): AnyRef = {
    v match {
      case map:JObject =>
        if (schema.getType == Type.RECORD) {
          val builder = new GenericRecordBuilder(schema)
          map.obj.foreach { p =>
            val fieldName = AvroUtils.formatName(p._1)
            schema.getField(fieldName) match {
              case field: Field =>
                val fSchema = field.schema
                val (realSchema, isArray) = fSchema.getType match {
                  case Type.UNION => (fSchema.getTypes.get(1), false)
                  case Type.ARRAY => (fSchema.getElementType, true)
                  case _ => (fSchema, false)
                }
                builder.set(fieldName, getValue(realSchema, p._2))
              case _ =>
            }
          }
          builder.build
        } else if (schema.getType == Type.MAP) {
          val mapOut = new java.util.HashMap[String, AnyRef]
          val mSchema = schema.getValueType
          map.obj.foreach { p =>
            mapOut.put(p._1, getValue(mSchema, p._2))
          }
          mapOut
        } else if (schema.getType == Type.STRING) {
          map.toString
        } else null
      case list:JArray => list.productIterator.toArray
      case null|JNull|JNothing => null
      case bool:JBool => java.lang.Boolean.valueOf(bool.value)
      case str:JString => str.values
      case num:JDouble => new java.lang.Double(num.values)
      case num:JInt =>
        schema.getType match {
          case Type.DOUBLE => new java.lang.Double(num.num.toDouble)
          case Type.INT => new java.lang.Integer(num.num.toInt)
          case Type.LONG => new java.lang.Long(num.num.toLong)
          case Type.FLOAT => new java.lang.Float(num.num.toFloat)
          case _ => num.num.toString
        }
      case num:JDecimal =>
        schema.getType match {
          case Type.DOUBLE => new java.lang.Double(num.num.toDouble)
          case Type.INT => new java.lang.Integer(num.num.toInt)
          case Type.LONG => new java.lang.Long(num.num.toLong)
          case Type.FLOAT => new java.lang.Float(num.num.toFloat)
          case _ => num.num.toString
        }
      case _ => null
    }
  }
}
