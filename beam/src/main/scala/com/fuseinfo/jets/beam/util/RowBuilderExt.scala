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

package com.fuseinfo.jets.beam.util

import org.apache.beam.sdk.values.Row

class RowBuilderExt(builder:Row.Builder) {
  def addBoolean(bool: Boolean): Row.Builder = builder.addValue(bool)
  def addBoolean(bool: java.lang.Boolean): Row.Builder = builder.addValue(bool)
  def addBoolean(bool: String): Row.Builder = builder.addValue(bool.toBoolean)

  def addByte(b: Byte): Row.Builder = builder.addValue(b)
  def addByte(b: java.lang.Byte): Row.Builder = builder.addValue(b)

  def addBytes(bytes: Array[Byte]): Row.Builder = builder.addValue(bytes)

  def addDateTime(dt: org.joda.time.ReadableInstant): Row.Builder = builder.addValue(dt)
  def addDateTime(dt: java.util.Date): Row.Builder = builder.addValue(new org.joda.time.DateTime(dt.getTime))
  def addDateTime(dt: Long): Row.Builder = builder.addValue(new org.joda.time.DateTime(dt))

  def addDecimal(dec: java.math.BigDecimal): Row.Builder = builder.addValue(dec)
  def addDecimal(dec: Long): Row.Builder = builder.addValue(java.math.BigDecimal.valueOf(dec))
  def addDecimal(dec: Int): Row.Builder = builder.addValue(java.math.BigDecimal.valueOf(dec))
  def addDecimal(dec: Double): Row.Builder = builder.addValue(java.math.BigDecimal.valueOf(dec))
  def addDecimal(dec: Float): Row.Builder = builder.addValue(java.math.BigDecimal.valueOf(dec))
  def addDecimal(dec: String): Row.Builder = builder.addValue(new java.math.BigDecimal(dec))

  def addDouble(db: Double): Row.Builder = builder.addValue(db)
  def addDouble(db: java.lang.Double): Row.Builder = builder.addValue(db)
  def addDouble(db: Long): Row.Builder = builder.addValue(db.toDouble)
  def addDouble(db: Int): Row.Builder = builder.addValue(db.toDouble)

  def addFloat(ft: Float): Row.Builder = builder.addValue(ft)
  def addFloat(ft: java.lang.Float): Row.Builder = builder.addValue(ft)
  def addFloat(ft: Int): Row.Builder = builder.addValue(ft.toFloat)

  def addInt16(i: Int): Row.Builder = builder.addValue(i.toShort)
  def addInt16(i: Short): Row.Builder = builder.addValue(i)
  def addInt16(i: java.lang.Short): Row.Builder = builder.addValue(i)

  def addInt32(i: Int): Row.Builder = builder.addValue(i)
  def addInt32(i: java.lang.Integer): Row.Builder = builder.addValue(i)

  def addInt64(i: Int): Row.Builder = builder.addValue(i.toLong)
  def addInt64(i: Long): Row.Builder = builder.addValue(i)
  def addInt64(i: java.lang.Long): Row.Builder = builder.addValue(i)

  def addString(s: String): Row.Builder = builder.addValue(s)
  def addString(): Row.Builder = builder.addValue("")
}

