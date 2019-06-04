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

package com.fuseinfo.jets.kafka.timer

import com.fasterxml.jackson.databind.node.ObjectNode
import com.fuseinfo.jets.kafka.KafkaFlowBuilder
import com.fuseinfo.jets.kafka.store.ProcessorStore
import com.fuseinfo.jets.kafka.util.JsonUtils
import com.fuseinfo.jets.util.VarUtils
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.streams.processor.{ProcessorContext, Punctuator}

class RetryTimer(context:ProcessorContext, punctuateFunc:(GenericRecord,GenericRecord,Boolean) => GenericRecord,
                 keyFunc:(GenericRecord, GenericRecord) => GenericRecord, params:ObjectNode,
                 stateStore:ProcessorStore) extends Punctuator {

  private val expireTime =
    VarUtils.enrichString(JsonUtils.getOrElse(params, "timeout", "5000"), KafkaFlowBuilder.vars).toLong
  private var timeFrom = 0L

  override def punctuate(timestamp: Long): Unit = {
    val iter = stateStore.fetch(stateStore.defaultKey, timeFrom, Long.MaxValue)
    var notBlocked = true
    while (iter.hasNext) {
      val kv = iter.next
      val time = kv.key
      val record = kv.value
      if (record != null) {
        val isExpired = System.currentTimeMillis - expireTime > time
        val keyRecord = keyFunc(null, record)
        val newRecord = punctuateFunc(keyRecord, record, isExpired)
        if (newRecord != null) {
          context.forward(keyRecord, newRecord)
        }
        if (newRecord != null || isExpired) {
          stateStore.put(stateStore.defaultKey, null, time)
          if (notBlocked) timeFrom = time + 1
        } else notBlocked = false
      }
    }
    iter.close()
  }

}
