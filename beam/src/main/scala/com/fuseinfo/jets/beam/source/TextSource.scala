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

package com.fuseinfo.jets.beam.source

import com.fuseinfo.jets.beam.{BeamFlowBuilder, BeamParser, NullBeamParser, SourceParser}
import com.fuseinfo.jets.util.AvroUtils
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.coders.{AvroCoder, KvCoder}
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.transforms.{MapElements, SimpleFunction}
import org.apache.beam.sdk.values.{KV, PCollection}

import scala.util.Try

class TextSource(paramNode:java.util.Map[String, AnyRef]) extends SourceParser {

  private val schemaText = paramNode.get("valueSchema") match {
    case null => paramNode.get("schema")
    case v => v
  }

  @transient private val valueSchema = new Schema.Parser().parse(schemaText.toString)


  override def getKeySchema: Schema = null

  override def getValueSchema: Schema = valueSchema

  override def apply(pipeline: Pipeline, processorName:String): PCollection[KV[GenericRecord, GenericRecord]] = {
    val reader = TextIO.read().from(paramNode.get("path").toString)

    val valueParserNode = paramNode.get("parser") match {
      case null => paramNode.get("valueParser").asInstanceOf[java.util.Map[String, AnyRef]]
      case node:java.util.Map[_, _] => node.asInstanceOf[java.util.Map[String, AnyRef]]
      case _ => null
    }
    if (valueParserNode == null) throw new IllegalArgumentException("Missing parser")

    val valueParser = createParser(valueParserNode, schemaText.toString, false)
    val strCollection = pipeline.apply(processorName, reader)
    strCollection.apply(processorName + ":parser", MapElements.via(new SimpleFunction[String, KV[GenericRecord, GenericRecord]]{
      override def apply(input: String): KV[GenericRecord, GenericRecord] =
        KV.of(null, valueParser(input.getBytes))
    })).setCoder(KvCoder.of(AvroCoder.of(AvroUtils.emptySchema), AvroCoder.of(getValueSchema)))
  }

  private def createParser(parserNode: java.util.Map[String, AnyRef], schema:String, isKey:java.lang.Boolean):BeamParser[Array[Byte]] = {
    if (schema != null) {
      val className = parserNode.get("__parser").toString
      val clazz = Try(Class.forName(className)).getOrElse(Class.forName(BeamFlowBuilder.packagePrefix + "parser." + className))
      clazz.getConstructor(classOf[java.util.Map[String, AnyRef]], classOf[String], classOf[java.lang.Boolean])
        .newInstance(parserNode, schema, isKey).asInstanceOf[BeamParser[Array[Byte]]]
    } else NullBeamParser.byteArray
  }

}
