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

package com.fuseinfo.jets.beam

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.transforms.{DoFn, PTransform, ProcessFunction, SimpleFunction}
import org.apache.beam.sdk.values.{KV, PCollection, PDone}

trait WithKeySchema {
  def getKeySchema: Schema
}

trait WithValueSchema {
  def getValueSchema: Schema
}

trait SourceParser extends ((Pipeline, String) => PCollection[KV[GenericRecord, GenericRecord]])
  with WithKeySchema with WithValueSchema

abstract class TransformerFn(keySchema: String)
  extends DoFn[KV[GenericRecord, GenericRecord], KV[GenericRecord, GenericRecord]]
    with WithKeySchema with WithValueSchema {

  def setStoreMap(storeDefMap:java.util.Map[String, java.util.Map[String, AnyRef]]):Unit = {}
}

abstract class MapFn extends SimpleFunction[KV[GenericRecord, GenericRecord], KV[GenericRecord, GenericRecord]]
  with WithKeySchema with WithValueSchema

abstract class KeyMapFn(valueSchema:String) extends MapFn

abstract class ValueMapFn(keySchema: String) extends MapFn

abstract class FilterFn(keySchema: String, valueSchema: String)
  extends ProcessFunction[KV[GenericRecord, GenericRecord], java.lang.Boolean] with WithKeySchema with WithValueSchema

trait BeamParser[T] extends (T => GenericRecord) with Serializable {
  def apply(t:T): GenericRecord
}

class NullBeamParser[T] extends BeamParser[T] {
  override def apply(t:T): GenericRecord = null
}

object NullBeamParser {
  val byteArray = new NullBeamParser[Array[Byte]]
}

trait BeamFormatter[T] extends (GenericRecord => T) with Serializable {
  def apply(record: GenericRecord):T
}

trait SinkFormatter extends PTransform[PCollection[KV[GenericRecord, GenericRecord]], PDone]

class NullBeamFormatter[T <: AnyRef] extends BeamFormatter[T] {
  override def apply(record:GenericRecord): T = null.asInstanceOf[T]
}

object NullBeamFormatter {
  val byteArray: BeamFormatter[Array[Byte]] = new BeamFormatter[Array[Byte]]{
    override def apply(v1: GenericRecord): Array[Byte] = Array.emptyByteArray
  }
}
