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
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.{KV, PCollection}
import org.apache.beam.sdk.io.kafka.KafkaIO
import org.apache.beam.sdk.transforms.{MapElements, SimpleFunction}
import org.apache.kafka.common.serialization.ByteArrayDeserializer

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.util.Try

class KafkaSource(paramNode:java.util.Map[String, AnyRef]) extends SourceParser {
  @transient private val parser = new Schema.Parser

  private val keySchemaStr = paramNode.get("keySchema") match {
    case null => None
    case node => Some(node.toString)
  }

  @transient private val keySchema:Option[Schema] = keySchemaStr.map(parser.parse)

  private val valueSchemaStr = paramNode("valueSchema").toString
  @transient private val valueSchema:Schema = parser.parse(valueSchemaStr)

  override def getKeySchema: Schema = keySchema.orNull

  override def getValueSchema: Schema = valueSchema

  override def apply(pipeline: Pipeline, processorName:String): PCollection[KV[GenericRecord, GenericRecord]] = {
    val opts = paramNode.filter(_._2.isInstanceOf[String]).map(p => p._1 -> p._2.asInstanceOf[String])
    val reservedSet = Set("bootstrap.servers","topic","key.deserializer","value.deserializer","valueSchema","keySchema")
    val reader = KafkaIO.read[Array[Byte], Array[Byte]].withBootstrapServers(opts("bootstrap.servers"))
        .withKeyDeserializer(classOf[ByteArrayDeserializer])
        .withValueDeserializer(classOf[ByteArrayDeserializer])
        .updateConsumerProperties(opts.filterKeys(k => !reservedSet.contains(k) && !k.startsWith("__")).asJava
          .asInstanceOf[java.util.Map[String, AnyRef]])
    val readerWithTopic = paramNode.get("topic") match {
      case topic:String => reader.withTopic(topic)
      case topics:Array[String] => reader.withTopics(java.util.Arrays.asList(topics:_*))
      case topic:AnyRef => reader.withTopic(topic.toString)
    }
    val (keyNode, valueNode) = paramNode.get("parser") match {
      case null =>
        ( paramNode.get("keyParser") match {
          case null => None
          case nd:java.util.Map[_, _] => Some(nd.asInstanceOf[java.util.Map[String, AnyRef]])
          case _ => None
        },paramNode.get("valueParser") match {
          case null => None
          case nd:java.util.Map[_, _] => Some(nd.asInstanceOf[java.util.Map[String, AnyRef]])
          case _ => None
        })
      case node:java.util.Map[_, _] =>
        (Some(node.asInstanceOf[java.util.Map[String, AnyRef]]), Some(node.asInstanceOf[java.util.Map[String, AnyRef]]))
      case _ => throw new IllegalArgumentException("parser is required")
    }
    val keyParser = keyNode.map(nd => createParser(nd, keySchemaStr.orNull, true)).getOrElse(NullBeamParser.byteArray)
    val valueParser = valueNode.map(nd => createParser(nd, valueSchemaStr, false)).getOrElse(NullBeamParser.byteArray)
    pipeline.apply(processorName, readerWithTopic.withoutMetadata)
      .apply(processorName + ":parser", MapElements.via(new SimpleFunction[KV[Array[Byte], Array[Byte]], KV[GenericRecord, GenericRecord]]{

      override def apply(input: KV[Array[Byte], Array[Byte]]): KV[GenericRecord, GenericRecord] =
        KV.of(keyParser(input.getKey), valueParser(input.getValue))
    }))
  }

  private def createParser(parserNode: java.util.Map[String, AnyRef], schema:String, isKey:java.lang.Boolean) = {
    if (schema != null) {
      val className = parserNode.get("__parser").toString
      parserNode.put("topic", paramNode.get("topic"))
      val clazz = Try(Class.forName(className)).getOrElse(Class.forName(BeamFlowBuilder.packagePrefix + "parser." + className))
      clazz.getConstructor(classOf[java.util.Map[String, AnyRef]], classOf[String], classOf[java.lang.Boolean])
        .newInstance(parserNode, schema, isKey).asInstanceOf[BeamParser[Array[Byte]]]
    } else NullBeamParser.byteArray
  }

}
