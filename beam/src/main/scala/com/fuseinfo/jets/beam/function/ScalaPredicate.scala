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

import com.fuseinfo.jets.beam.FilterFn
import com.fuseinfo.jets.beam.util.AvroFunctionFactory
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.values.KV

class ScalaPredicate(paramNode:java.util.Map[String, AnyRef], keySchema:String, valueSchema:String)
  extends FilterFn(keySchema, valueSchema) {

  @transient private lazy val parser = new Schema.Parser()
  @transient private lazy val keyOutSchema = if (keySchema != null) parser.parse(keySchema) else null
  @transient private lazy val valueOutSchema = parser.parse(valueSchema)

  private val test = paramNode.get("test").toString

  private lazy val testFunc = getTest(keyOutSchema, valueOutSchema, test).newInstance()

  private def getTest(keySchema:Schema, valueSchema:Schema, test:String) =
    AvroFunctionFactory.getTestClass(keySchema, valueSchema, test)

  override def apply(input: KV[GenericRecord, GenericRecord]): java.lang.Boolean =
    java.lang.Boolean.valueOf(testFunc(input.getKey, input.getValue))

  override def getKeySchema: Schema = keyOutSchema

  override def getValueSchema: Schema = valueOutSchema

}
