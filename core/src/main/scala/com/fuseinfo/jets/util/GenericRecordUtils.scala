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

package com.fuseinfo.jets.util

import java.nio.ByteBuffer

import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8

import scala.collection.JavaConversions._

class GenericRecordUtils(record:GenericRecord) {

  def getString(key:String): String = record.get(key) match {
    case null => null
    case data => data.toString
  }

  def getBoolean(key:String): Boolean = record.get(key).asInstanceOf[Boolean]

  def getBytes(key:String): ByteBuffer = record.get(key).asInstanceOf[ByteBuffer]

  def getFloat(key:String): java.lang.Float = record.get(key).asInstanceOf[java.lang.Float]

  def getInt(key: String): java.lang.Integer = record.get(key).asInstanceOf[java.lang.Integer]

  def getDouble(key: String): java.lang.Double = record.get(key).asInstanceOf[java.lang.Double]

  def getLong(key: String): java.lang.Long = record.get(key).asInstanceOf[java.lang.Long]

  def getFixed(key: String): Array[Byte] = record.get(key).asInstanceOf[Array[Byte]]

  def getMap[T](key: String): java.util.Map[String, T] = {
    val result = record.get(key).asInstanceOf[java.util.Map[CharSequence, AnyRef]]
    if (result != null && result.isInstanceOf[java.util.Map[_, _]]) {
      record.get(key).asInstanceOf[java.util.Map[CharSequence, AnyRef]].map { p =>
        val pv = p._2
        p._1.toString -> (if (pv != null && pv.isInstanceOf[Utf8]) pv.toString else pv).asInstanceOf[T]
      }
    } else null
  }

  def getArray[T](key: String): java.util.Collection[T] = record.get(key).asInstanceOf[java.util.Collection[T]]

  def getRecord(key: String): GenericRecord = record.get(key).asInstanceOf[GenericRecord]
}
