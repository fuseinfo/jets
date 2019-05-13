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

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

class RedisProcessorStore2(params: java.util.Map[String, String], keySchema:Schema, valueSchema:Schema)
  extends RedisProcessorStore(params, keySchema, valueSchema) {

  override def peekRetry(key: Array[Byte], timeTo:Long): (GenericRecord, Long, Array[Byte]) = {
    val jedis = jedisPool.getResource
    if (auth != null) jedis.auth(auth)
    val set = jedis.zrangeByScoreWithScores(key, 0, timeTo, 0, 1)
    val res = if (!set.isEmpty) {
      val tuple = set.iterator().next()
      val valueBinary = tuple.getBinaryElement
      jedis.zadd(key, System.currentTimeMillis, valueBinary)
      val (record, createdTime) = bytesToValueTS(valueBinary)
      (record, createdTime, valueBinary)
    } else null
    jedis.close()
    res
  }
}
