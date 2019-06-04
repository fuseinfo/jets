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

package com.fuseinfo.jets.kafka.store

import org.apache.avro.generic.GenericRecord
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Windowed
import org.apache.kafka.streams.processor.{ProcessorContext, StateStore}
import org.apache.kafka.streams.state.{KeyValueIterator, WindowStore, WindowStoreIterator}

import scala.collection.JavaConversions._

class ProcessorStore(inner:WindowStore[GenericRecord, GenericRecord], val defaultKey:GenericRecord,
                      val defaultValue:GenericRecord) extends WindowStore[GenericRecord, GenericRecord]{

  override def put(key: GenericRecord, value: GenericRecord): Unit = inner.put(key, value, System.currentTimeMillis)

  override def put(key: GenericRecord, value: GenericRecord, timestamp: Long): Unit = inner.put(key, value, timestamp)

  def putUnique(key: GenericRecord, value:GenericRecord): Unit = putUnique(key, value, System.currentTimeMillis)

  def putUnique(key: GenericRecord, value:GenericRecord, timestamp: Long): Unit = {
    var now = timestamp
    while (inner.fetch(key, now) != null) now += 1
    inner.put(key, value, now)
  }

  override def name(): String = inner.name

  override def init(context: ProcessorContext, root: StateStore): Unit = inner.init(context, root)

  override def flush(): Unit = inner.flush()

  override def close(): Unit = inner.close()

  override def persistent(): Boolean = inner.persistent()

  override def isOpen: Boolean = inner.isOpen

  override def fetch(key: GenericRecord, time: Long): GenericRecord = inner.fetch(key, time)

  override def fetch(key: GenericRecord, timeFrom: Long, timeTo: Long): WindowStoreIterator[GenericRecord] =
    inner.fetch(key, timeFrom, timeTo)

  override def fetch(from: GenericRecord, to: GenericRecord, timeFrom: Long, timeTo: Long):
    KeyValueIterator[Windowed[GenericRecord], GenericRecord] = inner.fetch(from, to, timeFrom, timeTo)

  override def all(): KeyValueIterator[Windowed[GenericRecord], GenericRecord] = inner.all()

  override def fetchAll(timeFrom: Long, timeTo: Long): KeyValueIterator[Windowed[GenericRecord], GenericRecord] =
    inner.fetchAll(timeFrom, timeTo)

  def fetchFirst(key:GenericRecord, defaultV:GenericRecord): GenericRecord = {
    fetchFirst(key, 0L, Long.MaxValue, (_:KeyValue[java.lang.Long, GenericRecord]) => true, defaultV)
  }

  def fetchFirst(key: GenericRecord, timeFrom: Long, timeTo:Long): GenericRecord = {
    fetchFirst(key, timeFrom, timeTo, (_:KeyValue[java.lang.Long, GenericRecord]) => true, null)
  }

  def fetchFirst(key: GenericRecord, timeFrom: Long, timeTo:Long, defaultV:GenericRecord): GenericRecord = {
    fetchFirst(key, timeFrom, timeTo, (_:KeyValue[java.lang.Long, GenericRecord]) => true, defaultV)
  }

  def fetchFirst(key:GenericRecord, timeFrom: Long, timeTo:Long, p: KeyValue[java.lang.Long, GenericRecord] => Boolean,
                 defaultV:GenericRecord): GenericRecord = {
    val iterator = inner.fetch(key, timeFrom, Long.MaxValue)
    val res = iterator.find(p).map(_.value).getOrElse(defaultV)
    iterator.close()
    res
  }

  def fetchFirst(key:GenericRecord, p: GenericRecord => Boolean = _=>true, timeFrom: Long = 0L,
                     timeTo:Long = Long.MaxValue, defaultV:GenericRecord = null): GenericRecord = {
    fetchFirst(key, timeFrom, timeTo, (c:KeyValue[java.lang.Long, GenericRecord]) => p(c.value), defaultV)
  }

  def fetchLast(key:GenericRecord, defaultV:GenericRecord): GenericRecord = {
    fetchLast(key, 0, Long.MaxValue, (_:KeyValue[java.lang.Long, GenericRecord]) => true, defaultV)
  }

  def fetchLast(key:GenericRecord, timeFrom:Long, timeTo:Long): GenericRecord = {
    fetchLast(key, timeFrom, timeTo, (_:KeyValue[java.lang.Long, GenericRecord]) => true, null)
  }

  def fetchLast(key:GenericRecord, timeFrom:Long, timeTo:Long, defaultV:GenericRecord): GenericRecord = {
    fetchLast(key, timeFrom, timeTo, (_:KeyValue[java.lang.Long, GenericRecord]) => true, defaultV)
  }

  def fetchLast(key:GenericRecord, timeFrom: Long, timeTo:Long, p: KeyValue[java.lang.Long, GenericRecord] => Boolean,
                defaultV:GenericRecord): GenericRecord = {
    val iterator = inner.fetch(key, timeFrom, timeTo)
    val matched = iterator.filter(p)
    val res = if (matched.isEmpty) defaultV
    else matched.reduce((_, b) => b).value
    iterator.close()
    res
  }

  def fetchLast(key:GenericRecord, p: GenericRecord => Boolean = _ => true, timeFrom:Long = 0,
                timeTo:Long = Long.MaxValue, defaultV:GenericRecord = null): GenericRecord = {
    fetchLast(key, timeFrom, timeTo, (c:KeyValue[java.lang.Long, GenericRecord]) => p(c.value), defaultV)
  }

}
