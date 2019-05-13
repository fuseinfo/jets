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

import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Windowed
import org.apache.kafka.streams.processor.{ProcessorContext, StateStore}
import org.apache.kafka.streams.state.{KeyValueIterator, WindowStore, WindowStoreIterator}

import scala.collection.JavaConversions._

class ProcessorStore[K, V](inner:WindowStore[K, V], val defaultKey:K, val defaultValue:V) extends WindowStore[K, V]{

  override def put(key: K, value: V): Unit = inner.put(key, value, System.currentTimeMillis)

  override def put(key: K, value: V, timestamp: Long): Unit = inner.put(key, value, timestamp)

  def putUnique(key: K, value:V): Unit = putUnique(key, value, System.currentTimeMillis)

  def putUnique(key: K, value:V, timestamp: Long): Unit = {
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

  override def fetch(key: K, time: Long): V = inner.fetch(key, time)

  override def fetch(key: K, timeFrom: Long, timeTo: Long): WindowStoreIterator[V] =
    inner.fetch(key, timeFrom, timeTo)

  override def fetch(from: K, to: K, timeFrom: Long, timeTo: Long):
    KeyValueIterator[Windowed[K], V] = inner.fetch(from, to, timeFrom, timeTo)

  override def all(): KeyValueIterator[Windowed[K], V] = inner.all()

  override def fetchAll(timeFrom: Long, timeTo: Long): KeyValueIterator[Windowed[K], V] =
    inner.fetchAll(timeFrom, timeTo)

  def fetchFirst(key:K, defaultV:V): V = {
    fetchFirst(key, 0L, Long.MaxValue, (_:KeyValue[java.lang.Long, V]) => true, defaultV)
  }

  def fetchFirst(key: K, timeFrom: Long, timeTo:Long): V = {
    fetchFirst(key, timeFrom, timeTo, (_:KeyValue[java.lang.Long, V]) => true,  null.asInstanceOf[V])
  }

  def fetchFirst(key: K, timeFrom: Long, timeTo:Long, defaultV:V): V = {
    fetchFirst(key, timeFrom, timeTo, (_:KeyValue[java.lang.Long, V]) => true, defaultV)
  }

  def fetchFirst(key:K, timeFrom: Long, timeTo:Long, p: KeyValue[java.lang.Long, V] => Boolean, defaultV:V): V = {
    val iterator = inner.fetch(key, timeFrom, Long.MaxValue)
    val res = iterator.find(p).map(_.value).getOrElse(defaultV)
    iterator.close()
    res
  }

  def fetchFirst(key:K, p: V => Boolean = _=>true, timeFrom: Long = 0L,
                     timeTo:Long = Long.MaxValue, defaultV:V = null.asInstanceOf[V]): V = {
    fetchFirst(key, timeFrom, timeTo, (c:KeyValue[java.lang.Long, V]) => p(c.value), defaultV)
  }

  def fetchLast(key:K, defaultV:V): V = {
    fetchLast(key, 0, Long.MaxValue, (_:KeyValue[java.lang.Long, V]) => true, defaultV)
  }

  def fetchLast(key:K, timeFrom:Long, timeTo:Long): V = {
    fetchLast(key, timeFrom, timeTo, (_:KeyValue[java.lang.Long, V]) => true, null.asInstanceOf[V])
  }

  def fetchLast(key:K, timeFrom:Long, timeTo:Long, defaultV:V): V = {
    fetchLast(key, timeFrom, timeTo, (_:KeyValue[java.lang.Long, V]) => true, defaultV)
  }

  def fetchLast(key:K, timeFrom: Long, timeTo:Long, p: KeyValue[java.lang.Long, V] => Boolean, defaultV:V): V = {
    val iterator = inner.fetch(key, timeFrom, timeTo)
    val matched = iterator.filter(p)
    val res = if (matched.isEmpty) defaultV
    else matched.reduce((_, b) => b).value
    iterator.close()
    res
  }

  def fetchLast(key:K, p: V => Boolean = _ => true, timeFrom:Long = 0,
                timeTo:Long = Long.MaxValue, defaultV:V = null.asInstanceOf[V]): V = {
    fetchLast(key, timeFrom, timeTo, (c:KeyValue[java.lang.Long, V]) => p(c.value), defaultV)
  }

}
