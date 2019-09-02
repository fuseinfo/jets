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

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import com.fuseinfo.jets.kafka.{ErrorHandler, KafkaFlowBuilder, SourceStream, StreamProcessor, StreamSink}
import org.apache.avro.Schema

import scala.collection.JavaConversions._

object JsonUtils {

  def getOrElse(jsonNode:JsonNode, name:String, defaultValue:String): String = {
    jsonNode.get(name) match {
      case null => defaultValue
      case json => json.asText
    }
  }

  def get(jsonNode:JsonNode, name:String): Option[String] = {
    jsonNode.get(name) match {
      case null => None
      case json => Some(json.asText)
    }
  }

  private def initInstance[T](className: String, prefix: String, paramNode: ObjectNode)(r: Class[_] => T) = {
    r(try {
      Class.forName(className)
    } catch {
      case _: ClassNotFoundException => Class.forName(prefix + className)
    })
  }

  private def initFunc[T](step: String, pack: String, rootNode: JsonNode, prefix: String)(r: Class[_] => T) = {
    rootNode match {
      case objectNode: ObjectNode => initInstance[T](objectNode.get("__" + pack).asText, prefix, objectNode)(r)
      case _ => initInstance[T](rootNode.asText, step, null)(r)
    }
  }

  def initSourceFunc(step: String, rootNode: JsonNode): SourceStream = {
    initFunc[SourceStream](step, "source", rootNode, KafkaFlowBuilder.packagePrefix + "source."){clazz =>
      try {
        clazz.getDeclaredConstructor(classOf[String], classOf[ObjectNode])
          .newInstance(step, rootNode).asInstanceOf[SourceStream]
      } catch {
        case _: NoSuchElementException =>
          clazz.getDeclaredConstructor(classOf[String]).newInstance(step).asInstanceOf[SourceStream]
      }
    }
  }

  def initProcessorFunc(step: String, rootNode: JsonNode, keySchema: Schema, valueSchema: Schema): StreamProcessor = {
    initFunc[StreamProcessor](step, "function", rootNode, KafkaFlowBuilder.packagePrefix + "function."){clazz =>
      try {
        clazz.getDeclaredConstructor(classOf[String], classOf[ObjectNode], classOf[Schema], classOf[Schema])
          .newInstance(step, rootNode, keySchema, valueSchema).asInstanceOf[StreamProcessor]
      } catch {
        case _: NoSuchElementException =>
          clazz.getDeclaredConstructor(classOf[String], classOf[ObjectNode], classOf[Schema])
            .newInstance(step, rootNode, valueSchema).asInstanceOf[StreamProcessor]
      }
    }
  }

  def initSinkFunc(step: String, rootNode: JsonNode, keySchema: Schema, valueSchema: Schema): StreamSink = {
    initFunc[StreamSink](step, "sink", rootNode, KafkaFlowBuilder.packagePrefix + "sink."){clazz =>
      try {
        clazz.getDeclaredConstructor(classOf[String], classOf[ObjectNode], classOf[Schema], classOf[Schema])
          .newInstance(step, rootNode, keySchema, valueSchema).asInstanceOf[StreamSink]
      } catch {
        case _: NoSuchElementException =>
          clazz.getDeclaredConstructor(classOf[String], classOf[ObjectNode], classOf[Schema])
            .newInstance(step, rootNode, valueSchema).asInstanceOf[StreamSink]
      }
    }
  }

  def initFuncArray[T](step: String, pack: String, root: JsonNode, prefix: String)(r:Class[_] => T): List[T] = {
    root match {
      case arrayNode: ArrayNode => arrayNode.elements.map(initFunc[T](step, pack, _, prefix)(r)).toList
      case _ => Nil
    }
  }

  def initErrorFuncs[T <: AnyRef](step: String, root: JsonNode): List[ErrorHandler[T]] = {
    initFuncArray[ErrorHandler[T]](step, "error", root, KafkaFlowBuilder.packagePrefix + "error."){clazz =>
      try {
        clazz.getDeclaredConstructor(classOf[String], classOf[ObjectNode])
          .newInstance(step, root).asInstanceOf[ErrorHandler[T]]
      } catch {
        case _: NoSuchElementException => try {
          clazz.getDeclaredConstructor(classOf[String]).newInstance(step).asInstanceOf[ErrorHandler[T]]
        } catch {
          case _: NoSuchElementException => clazz.newInstance().asInstanceOf[ErrorHandler[T]]
        }
      }
    }
  }
}
