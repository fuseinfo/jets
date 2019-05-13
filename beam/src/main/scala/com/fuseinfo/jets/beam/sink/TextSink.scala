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

package com.fuseinfo.jets.beam.sink

import com.fuseinfo.jets.beam.{BeamFlowBuilder, BeamFormatter, SinkFormatter}
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.transforms.{MapElements, SimpleFunction}
import org.apache.beam.sdk.values.{KV, PCollection, PDone}

import scala.util.Try

class TextSink(paramNode:java.util.Map[String, AnyRef], keySchema:String, valueSchema:String) extends SinkFormatter {

  override def expand(input: PCollection[KV[GenericRecord, GenericRecord]]): PDone = {
    val valueNode = paramNode.getOrDefault("formatter", paramNode.get("valueFormatter"))
    val valueFormatter = createFormatter(valueNode.asInstanceOf[java.util.Map[String, AnyRef]], valueSchema, false)
    val writer = TextIO.write().to(paramNode.get("path").toString)
    input.apply(MapElements.via(new SimpleFunction[KV[GenericRecord, GenericRecord], String](){
      override def apply(kv: KV[GenericRecord, GenericRecord]): String = new String(valueFormatter(kv.getValue))
    })).apply(writer)
  }

  private def createFormatter(node: java.util.Map[String, AnyRef], schema:String, isKey:java.lang.Boolean):
    BeamFormatter[Array[Byte]] = {
    val className = node.get("__formatter").toString
    val clazz = Try(Class.forName(className)).getOrElse(Class.forName(BeamFlowBuilder.packagePrefix + "formatter." + className))
    clazz.getConstructor(classOf[java.util.Map[String, AnyRef]], classOf[String], classOf[java.lang.Boolean])
      .newInstance(node, schema, isKey).asInstanceOf[BeamFormatter[Array[Byte]]]
  }

}
