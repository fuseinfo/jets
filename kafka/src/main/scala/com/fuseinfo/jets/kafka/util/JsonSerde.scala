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

import java.nio.ByteBuffer
import java.time.{Instant, LocalDate}
import java.time.format.DateTimeFormatter

import com.fuseinfo.jets.kafka.ErrorHandler
import com.fuseinfo.jets.util.AvroUtils
import javax.xml.bind.DatatypeConverter
import org.apache.avro.{LogicalType, LogicalTypes, Schema}
import org.apache.avro.Schema.{Field, Type}
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.json4s.{JArray, JBool, JDecimal, JDouble, JInt, JNothing, JNull, JObject, JString, JValue}
import org.json4s.jackson.JsonMethods

import scala.collection.JavaConversions._

class JsonSerde(var schema: Schema, onErrors: List[ErrorHandler[String]]) extends Serde[GenericRecord] {
  override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}

  override def serializer(): Serializer[GenericRecord] = new AvroJsonSerializer(onErrors)

  override def deserializer(): Deserializer[GenericRecord] = new AvroJsonDeserializer(schema, onErrors)
}

class AvroJsonSerializer(onErrors: List[ErrorHandler[String]]) extends Serializer[GenericRecord] {
  override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = {}

  override def serialize(topic: String, data: GenericRecord): Array[Byte] =
    if (data != null) data.toString.getBytes else null

  override def close(): Unit = {}
}

class AvroJsonDeserializer(schema: Schema, onErrors: List[ErrorHandler[String]]) extends Deserializer[GenericRecord] {
  override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = {}

  override def deserialize(topic: String, data: Array[Byte]): GenericRecord = try {
    getValue(schema, JsonMethods.parse(new String(data, "UTF-8"))).asInstanceOf[GenericRecord]
  } catch {
    case e: Throwable =>
      val value = data match {
        case null => ""
        case _ => DatatypeConverter.printHexBinary(data)
      }
      onErrors.foreach{errorHandler => errorHandler(e, null, value)}
      null
  }

  override def close(): Unit = {}

  private def getSchemaType(schema: Schema): (Schema.Type, LogicalType) = schema.getType match {
    case Type.UNION =>
      schema.getTypes.find(_.getType != Type.NULL).map(s => (s.getType, s.getLogicalType)).getOrElse((Type.NULL, null))
    case tp => (tp, schema.getLogicalType)
  }

  private def stringToInstant(str: String): Instant = Instant.from(DateTimeFormatter.ISO_INSTANT.parse(str))

  private def stringToLocalDate(str: String): LocalDate = LocalDate.parse(str)

  private def getValue(schema:Schema, v:JValue): AnyRef = {
    v match {
      case map:JObject =>
        if (schema.getType == Type.RECORD) {
          val builder = new GenericRecordBuilder(schema)
          map.obj.foreach { p =>
            val fieldName = AvroUtils.formatName(p._1)
            schema.getField(fieldName) match {
              case null =>
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
      case list:JArray =>
        val listOut = new java.util.ArrayList[AnyRef]
        val elementType = schema.getElementType
        list.arr.foreach(v => listOut.add(getValue(elementType, v)))
        listOut
      case null|JNull|JNothing => null
      case bool:JBool => java.lang.Boolean.valueOf(bool.value)
      case str:JString =>
        getSchemaType(schema) match {
          case (Type.BOOLEAN, _) => new java.lang.Boolean(str.values.toBoolean)
          case (Type.DOUBLE, _) => new java.lang.Double(str.values.toDouble)
          case (Type.INT, _:LogicalTypes.Date) => new java.lang.Integer(stringToLocalDate(str.values).toEpochDay.toInt)
          case (Type.INT, _) => new java.lang.Integer(str.values.toInt)
          case (Type.LONG, _:LogicalTypes.TimestampMillis) =>
            new java.lang.Long(stringToInstant(str.values).toEpochMilli)
          case (Type.LONG, _:LogicalTypes.TimestampMicros) =>
            val instant = stringToInstant(str.values)
            new java.lang.Long(instant.getEpochSecond * 1000000 + instant.getNano / 1000)
          case (Type.LONG, _) => new java.lang.Long(str.values.toLong)
          case (Type.FLOAT, _) => new java.lang.Float(str.values.toFloat)
          case (Type.BYTES, decimal: LogicalTypes.Decimal) =>
            val dec = new java.math.BigDecimal(str.values)
            ByteBuffer.wrap(dec.movePointRight(decimal.getScale).toBigInteger.toByteArray)
          case _ => str.values
        }
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