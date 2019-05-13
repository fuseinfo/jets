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
import java.sql.Timestamp
import java.time.{LocalDate, ZoneId}

import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.avro.generic.{GenericData, GenericRecord, GenericRecordBuilder}

class AvroBuilder(schema:Schema) extends GenericRecordBuilder(schema) {

  def setRecord(pos:Field, record:GenericRecord): GenericRecordBuilder = set(pos, record)

  def setArray(pos:Field, array:java.util.Collection[AnyRef]): GenericRecordBuilder = set(pos, array)

  def setArray(pos:Field, array:Array[AnyRef]): GenericRecordBuilder = set(pos, java.util.Arrays.asList(array))

  def setBoolean(pos:Field, bool:java.lang.Boolean): GenericRecordBuilder = set(pos, bool)

  def setBytes(pos:Field, bytes:java.nio.ByteBuffer): GenericRecordBuilder = set(pos, bytes)

  def setDouble(pos:Field, double:java.lang.Double): GenericRecordBuilder = set(pos, double)

  def setFloat(pos:Field, float:java.lang.Float): GenericRecordBuilder = set(pos, float)

  def setInt(pos:Field, int:java.lang.Integer): GenericRecordBuilder = set(pos, int)

  def setLong(pos:Field, long:java.lang.Long): GenericRecordBuilder = set(pos, long)

  def setMap(pos:Field, map:java.util.Map[String, _ <: AnyRef]): GenericRecordBuilder = set(pos, map)

  def setString(pos:Field, str:CharSequence): GenericRecordBuilder = set(pos, str)

  def setDecimalBytes(pos:Field, double:Double, scale:Int): GenericRecordBuilder = {
    val data = scala.util.Try{
      ByteBuffer.wrap(BigInt(Math.round(Math.pow(10, scale) * double)).toByteArray)
    }.getOrElse(null)
    set(pos, data)
  }

  def setDecimalBytes(pos:Field, decimal:java.math.BigDecimal): GenericRecordBuilder = {
    val data = if (decimal != null) ByteBuffer.wrap(decimal.unscaledValue().toByteArray) else null
    set(pos, data)
  }

  def setDecimalFixed(pos:Field, double:Double, scale:Int, schema:Schema): GenericRecordBuilder = {
    val data = scala.util.Try{
      new GenericData.Fixed(schema, BigInt(Math.round(Math.pow(10, scale) * double)).toByteArray)
    }.getOrElse(null)
    set(pos, data)
  }

  def setDecimalFixed(pos:Field, decimal:java.math.BigDecimal, schema:Schema): GenericRecordBuilder = {
    val data = if (decimal != null) new GenericData.Fixed(schema, decimal.unscaledValue().toByteArray) else null
    set(pos, data)
  }

  def setTimestampMillis(pos:Field, long:java.lang.Long): GenericRecordBuilder = set(pos, long)

  def setTimestampMillis(pos:Field, ts:java.util.Date): GenericRecordBuilder = set(pos, ts.getTime)

  def setTimestampMillis(pos:Field, str:String): GenericRecordBuilder = {
    setTimestampMillis(pos, Timestamp.valueOf(str))
  }

  def setTimestampMicros(pos:Field, long:java.lang.Long): GenericRecordBuilder = set(pos, long)

  def setTimestampMicros(pos:Field, ts:Timestamp): GenericRecordBuilder = {
    set(pos, ts.getTime * 1000 + (ts.getNanos / 1000 % 1000))
  }

  def setTimestampMicros(pos:Field, str:String): GenericRecordBuilder = {
    setTimestampMicros(pos, Timestamp.valueOf(str))
  }

  def setDate(pos:Field, int:java.lang.Integer): GenericRecordBuilder = set(pos, int)

  def setDate(pos:Field, date:java.util.Date): GenericRecordBuilder = {
    set(pos, date.toInstant.atZone(ZoneId.systemDefault).toLocalDate.toEpochDay.toInt)}

  def setDate(pos:Field, str:String): GenericRecordBuilder = {
    set(pos, LocalDate.parse(str).toEpochDay.toInt)
  }
}
