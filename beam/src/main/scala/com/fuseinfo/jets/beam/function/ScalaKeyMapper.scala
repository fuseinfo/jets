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

package com.fuseinfo.jets.beam.function

import com.fuseinfo.jets.beam.KeyMapFn
import com.fuseinfo.jets.beam.util.AvroFunctionFactory
import com.fuseinfo.jets.util.AvroUtils
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.values.KV

class ScalaKeyMapper(paramNode:java.util.Map[String, AnyRef], keySchema:String, valueSchema:String)
    extends KeyMapFn(valueSchema) {

  @transient private lazy val parser = new Schema.Parser()

  @transient private lazy val valueOutSchema = parser.parse(valueSchema)

  @transient private lazy val keyOutSchema = paramNode.get("schema") match {
    case null => null
    case valueNode => (new Schema.Parser).parse(valueNode.toString)
  }

  private lazy val keyFunc: (GenericRecord, GenericRecord) => GenericRecord =
    getFunc(stringToSchema(keySchema), valueOutSchema, keyOutSchema,
      paramNode.get("keyMapping").asInstanceOf[java.util.Map[String, AnyRef]])

  private def getFunc(keySchema:Schema, valueSchema:Schema, outSchema:Schema, mapping:java.util.Map[String,AnyRef])= {
    val schemaName = "_schema"
    AvroFunctionFactory.getFuncClass[(GenericRecord,GenericRecord) => GenericRecord](keySchema, valueSchema, outSchema,
      mapping, 0, s"($schemaName:org.apache.avro.Schema) extends ((org.apache.avro.generic.GenericRecord" +
        ",org.apache.avro.generic.GenericRecord) => org.apache.avro.generic.GenericRecord)",
      "(_keyRecord:GenericRecord,_valueRecord:GenericRecord):GenericRecord", schemaName).
      getConstructor(classOf[Schema]).newInstance(outSchema)
  }

  override def apply(input: KV[GenericRecord, GenericRecord]): KV[GenericRecord, GenericRecord] =
    KV.of(keyFunc(input.getKey, input.getValue), input.getValue)

  override def getKeySchema: Schema = keyOutSchema

  override def getValueSchema: Schema = valueOutSchema

  private def stringToSchema(str: String): Schema = if (str != null) parser.parse(str) else AvroUtils.emptySchema
}
