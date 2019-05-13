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

package com.fuseinfo.jets.beam.timer

import com.fuseinfo.jets.beam.store.ProcessorStore
import org.apache.avro.generic.GenericRecord

class RetryTimer(punctuateFunc:(GenericRecord, GenericRecord, Boolean) => GenericRecord,
                 keyFunc:(GenericRecord, GenericRecord) => GenericRecord,
                 params:java.util.Map[String, AnyRef], store:ProcessorStore, retryKey:GenericRecord, interval:java.lang.Long)
  extends (((GenericRecord, GenericRecord) => Unit) => Unit) with Serializable {

  private val expireTime = params.getOrDefault("timeout", "5000").toString.toLong

  override def apply(output: (GenericRecord, GenericRecord) => Unit): Unit = {
    var tuple = store.peekRetry(retryKey, System.currentTimeMillis - interval)
    while (tuple != null) {
      val row = tuple._1
      val timestamp = tuple._2
      val isExpired = System.currentTimeMillis - expireTime > timestamp
      val keyRow = keyFunc(null, row)
      val outRow = punctuateFunc(keyRow, row, isExpired)
      if (outRow != null && store.removeRetry(retryKey, tuple._3) > 0) output(keyRow, outRow)
      tuple = store.peekRetry(retryKey, System.currentTimeMillis - interval)
    }
  }
}
