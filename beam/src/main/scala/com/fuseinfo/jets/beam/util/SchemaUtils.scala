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

package com.fuseinfo.jets.beam.util

import org.apache.beam.sdk.schemas.Schema
import org.apache.avro
import org.apache.avro.LogicalTypes

import scala.collection.JavaConversions._

object SchemaUtils {

  def avroSchemaToBeamSchema(avroSchema: avro.Schema): Schema = {
    val builder = new Schema.Builder
    avroSchema.getFields.foreach{field => builder.addField(field.name, avroTypeToBeamType(field.schema))}
    builder.build()
  }

  private def avroTypeToBeamType(avroSchema:avro.Schema):Schema.FieldType = {
    val logicalType = avroSchema.getLogicalType
    avroSchema.getType match {
      case avro.Schema.Type.ARRAY =>
        val elementType = avroTypeToBeamType(avroSchema.getElementType)
        Schema.FieldType.array(elementType)
      case avro.Schema.Type.BOOLEAN => Schema.FieldType.BOOLEAN
      case avro.Schema.Type.BYTES =>
        if (logicalType != null && logicalType.isInstanceOf[LogicalTypes.Decimal]) Schema.FieldType.DECIMAL
        else Schema.FieldType.BYTES
      case avro.Schema.Type.DOUBLE => Schema.FieldType.DOUBLE
      case avro.Schema.Type.ENUM => Schema.FieldType.STRING
      case avro.Schema.Type.FIXED =>
        if (logicalType != null && logicalType.isInstanceOf[LogicalTypes.Decimal]) Schema.FieldType.DECIMAL
        else Schema.FieldType.BYTES
      case avro.Schema.Type.FLOAT => Schema.FieldType.FLOAT
      case avro.Schema.Type.INT => logicalType match {
        case null => Schema.FieldType.INT32
        case _ => Schema.FieldType.DATETIME
      }
      case avro.Schema.Type.LONG => logicalType match {
        case null => Schema.FieldType.INT64
        case _ => Schema.FieldType.DATETIME
      }
      case avro.Schema.Type.MAP =>
        val valueType = avroTypeToBeamType(avroSchema.getValueType)
        Schema.FieldType.map(Schema.FieldType.STRING, valueType)
      case avro.Schema.Type.NULL => throw new IllegalArgumentException("null type only supported in union")
      case avro.Schema.Type.RECORD =>
        Schema.FieldType.row(avroSchemaToBeamSchema(avroSchema))
      case avro.Schema.Type.STRING => Schema.FieldType.STRING
      case avro.Schema.Type.UNION =>
        val realSchema = avroSchema.getTypes.find(_.getType != avro.Schema.Type.NULL).get
        avroTypeToBeamType(realSchema).withNullable(true)
    }
  }
}
