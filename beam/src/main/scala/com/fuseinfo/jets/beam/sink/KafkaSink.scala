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

import com.fuseinfo.jets.beam.{BeamFlowBuilder, BeamFormatter, NullBeamFormatter, SinkFormatter}
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.io.kafka.KafkaIO
import org.apache.beam.sdk.transforms.{MapElements, SimpleFunction}
import org.apache.beam.sdk.values.{KV, PCollection, PDone}
import org.apache.kafka.common.serialization.ByteArraySerializer

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.util.Try

class KafkaSink(paramNode:java.util.Map[String, AnyRef], keySchema:String, valueSchema:String) extends SinkFormatter {
  override def expand(input: PCollection[KV[GenericRecord, GenericRecord]]): PDone = {
    val (keyFormatterNode, valueFormatterNode) = paramNode.get("formatter") match {
      case null => (paramNode.get("keyFormatter"), paramNode.get("valueFormatter"))
      case node => (node, node)
    }
    val keyFormatter = keyFormatterNode match {
      case null => NullBeamFormatter.byteArray
      case node: java.util.Map[_, _] =>
        createFormatter(node.asInstanceOf[java.util.Map[String, AnyRef]], keySchema, true)
      case _ => NullBeamFormatter.byteArray
    }
    val valueFormatter = valueFormatterNode match {
        case null => NullBeamFormatter.byteArray
        case node: java.util.Map[_, _] =>
          createFormatter(node.asInstanceOf[java.util.Map[String, AnyRef]], valueSchema, false)
        case _ => NullBeamFormatter.byteArray
    }
    val reservedSet = Set("bootstrap.servers", "topic", "key.serializer", "value.serializer")
    val opts = paramNode.filter(_._2.isInstanceOf[String]).mapValues(_.asInstanceOf[String])
    val writer = KafkaIO.write[Array[Byte], Array[Byte]]()
        .withBootstrapServers(opts("bootstrap.servers"))
        .withTopic(opts("topic"))
        .withKeySerializer(classOf[ByteArraySerializer])
        .withValueSerializer(classOf[ByteArraySerializer])
        .updateProducerProperties(opts.filterKeys(k => !reservedSet.contains(k) && !k.startsWith("__")).asJava
          .asInstanceOf[java.util.Map[String, AnyRef]])

    input.apply(MapElements.via(new SimpleFunction[KV[GenericRecord, GenericRecord], KV[Array[Byte], Array[Byte]]](){
      override def apply(kv: KV[GenericRecord, GenericRecord]): KV[Array[Byte], Array[Byte]] =
        KV.of(keyFormatter(kv.getKey), valueFormatter(kv.getValue))
    })).apply(writer)
  }

  private def createFormatter(node: java.util.Map[String, AnyRef], schema:String, isKey:java.lang.Boolean):
    BeamFormatter[Array[Byte]] = {
    if (schema != null) {
      val className = node.get("__formatter").toString
      node.put("topic", paramNode.get("topic"))
      val clazz = Try(Class.forName(className)).getOrElse(Class.forName(BeamFlowBuilder.packagePrefix + "formatter." + className))
      clazz.getConstructor(classOf[java.util.Map[String, AnyRef]], classOf[String], classOf[java.lang.Boolean])
        .newInstance(node, schema, isKey).asInstanceOf[BeamFormatter[Array[Byte]]]
    } else NullBeamFormatter.byteArray
  }

}
