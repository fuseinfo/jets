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

import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.state.WindowStore

import scala.collection.JavaConversions._

class WindowStoreUtil[K <: AnyRef, V <: AnyRef](store: WindowStore[K, V]) {

  def fetchFirst(key:K): V = {
    fetchFirstFrom(key, 0, null.asInstanceOf[V])
  }

  def fetchFirst(key:K, defaultV:V): V = {
    fetchFirstFrom(key, 0, defaultV)
  }

  def fetchFirst(key:K, defaultV:V, p: KeyValue[java.lang.Long, V] => Boolean): V = {
    fetchFirstFrom(key, 0, defaultV, p)
  }

  def fetchFirst(key:K, p: V => Boolean): V = {
    fetchFirstFrom(key, 0, p, null.asInstanceOf[V])
  }

  def fetchFirst(key:K, p: V => Boolean, defaultV:V): V = {
    fetchFirstFrom(key, 0, p, defaultV)
  }

  def fetchFirst(key:K, defaultV:V, window:Long): V = {
    fetchFirstFrom(key, System.currentTimeMillis - window, defaultV)
  }

  def fetchFirst(key:K, defaultV:V, window:Long, p: KeyValue[java.lang.Long, V] => Boolean): V = {
    fetchFirstFrom(key, System.currentTimeMillis - window, defaultV, p)
  }

  def fetchFirst(key:K, p: V => Boolean, defaultV:V, window:Long): V = {
    fetchFirstFrom(key, System.currentTimeMillis - window, p, defaultV)
  }

  def fetchLast(key:K): V = {
    fetchLastFrom(key, 0, null.asInstanceOf[V])
  }

  def fetchLast(key:K, defaultV:V): V = {
    fetchLastFrom(key, 0, defaultV)
  }

  def fetchLast(key:K, defaultV:V, p: KeyValue[java.lang.Long, V] => Boolean): V = {
    fetchLastFrom(key, 0, defaultV, p)
  }

  def fetchLast(key:K, p: V => Boolean): V = {
    fetchLastFrom(key, 0, p, null.asInstanceOf[V])
  }

  def fetchLast(key:K, p: V => Boolean, defaultV:V): V = {
    fetchLastFrom(key, 0, p, defaultV)
  }

  def fetchLast(key:K, defaultV:V, window:Long): V = {
    fetchLastFrom(key, System.currentTimeMillis - window, defaultV)
  }

  def fetchLast(key:K, defaultV:V, window:Long, p: KeyValue[java.lang.Long, V] => Boolean): V = {
    fetchLastFrom(key, System.currentTimeMillis - window, defaultV, p)
  }

  def fetchLast(key:K, p: V => Boolean, defaultV:V, window:Long): V = {
    fetchLastFrom(key, System.currentTimeMillis - window, p, defaultV)
  }

  def fetchFirstFrom(key:K, timeFrom:Long, defaultV:V, p: KeyValue[java.lang.Long, V] => Boolean = _ => true): V = {
    val iterator = store.fetch(key, timeFrom, Long.MaxValue)
    val res = iterator.find(p).map(_.value).getOrElse(defaultV)
    iterator.close()
    res
  }

  def fetchLastFrom(key:K, timeFrom:Long, defaultV:V, p: KeyValue[java.lang.Long, V] => Boolean = _ => true): V = {
    val iterator = store.fetch(key, timeFrom, Long.MaxValue)
    val matched = iterator.filter(p)
    val res = if (matched.isEmpty) defaultV
    else matched.reduce((a,b) => b).value
    iterator.close()
    res
  }

  def fetchFirstFrom(key:K, timeFrom:Long, p: V => Boolean, defaultV:V): V = {
    fetchFirstFrom(key, timeFrom, defaultV, (c:KeyValue[java.lang.Long, V]) => p(c.value))
  }

  def fetchLastFrom(key:K, timeFrom:Long, p: V => Boolean, defaultV:V): V = {
    fetchLastFrom(key, timeFrom, defaultV, (c:KeyValue[java.lang.Long, V]) => p(c.value))
  }

}
