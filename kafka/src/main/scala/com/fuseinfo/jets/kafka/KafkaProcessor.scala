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

package com.fuseinfo.jets.kafka

import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.streams.{KeyValue, StreamsBuilder}
import org.apache.kafka.streams.kstream.{KStream, KeyValueMapper, Predicate, TransformerSupplier, ValueMapper}
import org.slf4j.Logger

trait WithKeySchema {
  def getKeySchema: Schema
}

trait WithValueSchema {
  def getValueSchema: Schema
}

abstract class SourceStream extends StreamProcessor
  with (StreamsBuilder => KStream[GenericRecord, GenericRecord]) with WithKeySchema with WithValueSchema

abstract class SchemaTransformerSupplier extends StreamProcessor
  with TransformerSupplier[GenericRecord, GenericRecord, KeyValue[GenericRecord, GenericRecord]]
  with WithValueSchema {
  def setStoreMap(storeMap: Map[String, String]): Unit = {}
}

abstract class BranchPredicates extends StreamProcessor
  with Seq[Predicate[GenericRecord, GenericRecord]]

abstract class SplitMapper extends StreamProcessor
  with (KStream[GenericRecord, GenericRecord] => Map[String, KStream[GenericRecord, GenericRecord]]) {
  def getValueSchema(child: String): Schema
}

abstract class AvroPredicate extends StreamProcessor
  with Predicate[GenericRecord, GenericRecord]

abstract class FlatKeyValueMapper extends StreamProcessor
  with KeyValueMapper[GenericRecord, GenericRecord, java.lang.Iterable[KeyValue[GenericRecord, GenericRecord]]]
  with WithKeySchema with WithValueSchema

abstract class FlatValueMapper extends StreamProcessor
  with ValueMapper[GenericRecord, java.lang.Iterable[GenericRecord]] with WithValueSchema

abstract class SingleKeyValueMapper extends StreamProcessor
  with KeyValueMapper[GenericRecord, GenericRecord, KeyValue[GenericRecord, GenericRecord]]
  with WithKeySchema with WithValueSchema

abstract class SingleValueMapper extends StreamProcessor
  with ValueMapper[GenericRecord, GenericRecord] with WithValueSchema

abstract class KeyMapper extends StreamProcessor
  with KeyValueMapper[GenericRecord, GenericRecord, GenericRecord] with WithKeySchema

abstract class SinkKeyValueMapper extends StreamProcessor
  with KeyValueMapper[GenericRecord, GenericRecord, KeyValue[Array[Byte], Array[Byte]]] {
  override def getEventCount = 0L
}

abstract class StreamSink extends StreamProcessor with (KStream[GenericRecord, GenericRecord] => Unit)

trait ErrorHandler[T <: AnyRef] extends ((Throwable, T, T) => T)

class ErrorLogger[T <: AnyRef](stepName:String, logger: Logger) extends ErrorHandler[T] {
  override def apply(e: Throwable, key: T, value: T): T = {
    logger.error(s"Key:${String.valueOf(key)}\nValue:${String.valueOf(value)}", e)
    null.asInstanceOf[T]
  }
}

trait EventCounter {
  def getEventCount:Long = 0L
  def getEventCount(idx: Int): Long = getEventCount
}

trait StreamProcessor extends EventCounter{
  def getProcessorSchema:String

  def reset(objNode: ObjectNode):Boolean = false
}

