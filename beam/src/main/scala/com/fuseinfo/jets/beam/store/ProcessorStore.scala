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

package com.fuseinfo.jets.beam.store

import org.apache.avro.generic.GenericRecord

abstract class ProcessorStore extends StateStore {

  def init():Unit = {}

  def put(key: GenericRecord, value: GenericRecord):Unit = put(key, value, System.currentTimeMillis)

  def put(key: GenericRecord, value: GenericRecord, timestamp: Long):Unit

  def putRetry(key: GenericRecord, value: GenericRecord, timestamp: Long): Unit

  def peekRetry(key: GenericRecord, timeTo:Long): (GenericRecord, Long, Array[Byte])

  def removeRetry(key: GenericRecord, value: Array[Byte]): Long

  def fetch(key: GenericRecord): Iterator[GenericRecord] = fetch(key, 0, Long.MaxValue)

  def fetch(key: GenericRecord, timeFrom: Long, timeTo: Long): Iterator[GenericRecord]

  def fetchFirst(key: GenericRecord): GenericRecord = fetchFirst(key, 0, Long.MaxValue, null)

  def fetchFirst(key: GenericRecord, defaultV:GenericRecord):GenericRecord = fetchFirst(key, 0, Long.MaxValue, defaultV)

  def fetchFirst(key: GenericRecord, timeFrom: Long): GenericRecord = fetchFirst(key, timeFrom, Long.MaxValue, null)

  def fetchFirst(key: GenericRecord, timeFrom: Long, defaultV:GenericRecord): GenericRecord =
    fetchFirst(key, timeFrom, Long.MaxValue, defaultV)

  def fetchFirst(key: GenericRecord, timeFrom:Long, timeTo:Long):GenericRecord = fetchFirst(key, timeFrom, timeTo, null)

  def fetchFirst(key: GenericRecord, timeFrom: Long, timeTo:Long, defaultV:GenericRecord): GenericRecord

  def fetchLast(key: GenericRecord): GenericRecord = fetchLast(key, 0, Long.MaxValue, null)

  def fetchLast(key: GenericRecord, defaultV:GenericRecord): GenericRecord = fetchLast(key, 0, Long.MaxValue, defaultV)

  def fetchLast(key: GenericRecord, timeFrom: Long): GenericRecord = fetchLast(key, timeFrom, Long.MaxValue, null)

  def fetchLast(key: GenericRecord, timeFrom: Long, defaultV:GenericRecord): GenericRecord =
    fetchLast(key, timeFrom, Long.MaxValue, defaultV)

  def fetchLast(key: GenericRecord, timeFrom: Long, timeTo:Long): GenericRecord = fetchLast(key, timeFrom, timeTo, null)

  def fetchLast(key: GenericRecord, timeFrom: Long, timeTo:Long, defaultV:GenericRecord): GenericRecord

  def defaultValue:GenericRecord
}
/*
object ProcessorStore {
  def newInstance(keySchema: Schema, valueSchema: Schema):ProcessorStore = {
    val params = new java.util.HashMap[String, String]()
    new RedisProcessorStore(params, keySchema.toString, valueSchema.toString)
  }
}*/

trait StateStore extends Serializable