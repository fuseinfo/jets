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

package com.fuseinfo.jets

import org.apache.avro.generic.GenericRecord

package object util {
  implicit def String2Integer(s: String): Integer = new java.lang.Integer(s)

  implicit def String2Long(s: String): java.lang.Long = new java.lang.Long(s)

  implicit def String2Double(s: String): java.lang.Double = new java.lang.Double(s)

  implicit def String2Float(s: String): java.lang.Float = new java.lang.Float(s)

  implicit def String2Boolean(s: String): Boolean = new java.lang.Boolean(s)

  implicit def Int2Long(i: Int): java.lang.Long = new java.lang.Long(i)

  implicit def Int2Double(i: Int): java.lang.Double = new java.lang.Double(i)

  implicit def Int2Float(i: Int): java.lang.Float = new java.lang.Float(i)

  implicit def Long2Double(l: Long): java.lang.Double = new java.lang.Double(l)

  implicit def Long2Float(l: Long): java.lang.Float = new java.lang.Float(l)

  implicit def Float2Double(f: Float): java.lang.Double = new java.lang.Double(f)

  implicit def Double2Float(d: Double): java.lang.Float = new java.lang.Float(d)

  implicit def AnyRef2AvroLogical(obj: AnyRef): AvroUtils = new AvroUtils(obj)

  implicit def ExtendGenericRecord(record: GenericRecord): GenericRecordUtils = new GenericRecordUtils(record)
}
