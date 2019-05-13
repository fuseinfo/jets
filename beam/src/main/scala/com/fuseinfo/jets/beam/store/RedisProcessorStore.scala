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

import java.io.ByteArrayOutputStream

import com.fuseinfo.common.io.ByteBufferInputStream
import com.fuseinfo.jets.util.AvroUtils
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.coders.AvroCoder
import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool}
import org.apache.commons.pool2.{BasePooledObjectFactory, PooledObject}
import redis.clients.jedis.{BuilderFactory, Jedis, JedisPool, JedisPoolConfig, Tuple}
import redis.clients.jedis.commands.ProtocolCommand
import redis.clients.jedis.util.SafeEncoder

import scala.collection.JavaConversions._

class RedisProcessorStore(params: java.util.Map[String, String], keySchema:Schema, valueSchema:Schema)
  extends ProcessorStore {

  private val host = params.getOrDefault("host", "127.0.0.1").toString
  private val port = params.getOrDefault("port", "6379").toString.toInt
  private val ssl = params.getOrDefault("ssl", "false").toString.toBoolean
  protected val auth: String = params.get("auth")

  @transient protected lazy val jedisPool: JedisPool = {
    new JedisPool(new JedisPoolConfig, host, port, ssl)
  }

  @transient private lazy val keyCoder = AvroCoder.of(keySchema)
  @transient private lazy val valueCoder = AvroCoder.of(valueSchema)
  @transient private lazy val bufPool = new GenericObjectPool[ByteArrayOutputStream](new ByteArrayOutputStreamFactory)

  override def put(key: GenericRecord, value: GenericRecord, timestamp: Long): Unit = {
    val jedis = jedisPool.getResource
    if (auth != null) jedis.auth(auth)
    jedis.zadd(keyToBytes(key), timestamp, valueToBytes(value))
    jedis.close()
  }

  override def putRetry(key: GenericRecord, value: GenericRecord, timestamp: Long): Unit = {
    val jedis = jedisPool.getResource
    if (auth != null) jedis.auth(auth)
    jedis.zadd(keyToBytes(key), System.currentTimeMillis, valueToBytes(value, timestamp))
    jedis.close()
  }

  override def peekRetry(key: GenericRecord, timeTo:Long): (GenericRecord, Long, Array[Byte]) = {
    peekRetry(keyToBytes(key), timeTo)
  }

  def peekRetry(key: Array[Byte], timeTo:Long): (GenericRecord, Long, Array[Byte]) = {
    val jedis = jedisPool.getResource
    if (auth != null) jedis.auth(auth)
    val set = jedis.zrangeByScoreWithScores(key, 0, timeTo, 0, 1)
    val res = if (!set.isEmpty) {
      val tuple = zpopmin(jedis, key)
      if (tuple != null) {
        val valueBinary = tuple.getBinaryElement
        jedis.zadd(key, System.currentTimeMillis, valueBinary)
        if (tuple.getScore < timeTo) {
          val (record, createdTime) = bytesToValueTS(valueBinary)
          (record, createdTime, valueBinary)
        } else null
      } else null
    } else null
    jedis.close()
    res
  }

  def removeRetry(key: Array[Byte], value:Array[Byte]):Long = {
    val jedis = jedisPool.getResource
    if (auth != null) jedis.auth(auth)
    val res = jedis.zrem(key, value)
    jedis.close()
    res
  }

  override def removeRetry(key: GenericRecord, value: Array[Byte]): Long = removeRetry(keyToBytes(key), value)

  override def fetch(key: GenericRecord): Iterator[GenericRecord] = {
    val jedis = jedisPool.getResource
    if (auth != null) jedis.auth(auth)
    val res = jedis.zrange(keyToBytes(key), 0, -1).map(bytesToValue).iterator
    jedis.close()
    res
  }

  override def fetch(key: GenericRecord, timeFrom:Long, timeTo:Long): Iterator[GenericRecord] = {
    val jedis = jedisPool.getResource
    if (auth != null) jedis.auth(auth)
    val res = jedis.zrangeByScore(keyToBytes(key), timeFrom, timeTo).map(bytesToValue).toIterator
    jedis.close()
    res
  }

  override def fetchFirst(key: GenericRecord, defaultV:GenericRecord): GenericRecord = {
    val jedis = jedisPool.getResource
    if (auth != null) jedis.auth(auth)
    val set = jedis.zrange(keyToBytes(key), 0, 0)
    val res = if (set.isEmpty) defaultV else bytesToValue(set.iterator().next)
    jedis.close()
    res
  }

  override def fetchFirst(key: GenericRecord, timeFrom:Long, timeTo:Long, defaultV:GenericRecord): GenericRecord = {
    val jedis = jedisPool.getResource
    if (auth != null) jedis.auth(auth)
    val set = jedis.zrangeByScore(keyToBytes(key), timeFrom, timeTo)
    val res = if (set.isEmpty) defaultV else bytesToValue(set.iterator().next)
    jedis.close()
    res
  }

  override def fetchLast(key: GenericRecord, defaultV: GenericRecord): GenericRecord = {
    val jedis = jedisPool.getResource
    if (auth != null) jedis.auth(auth)
    val set = jedis.zrange(keyToBytes(key), -1, -1)
    val res = if (set.isEmpty) defaultV else bytesToValue(set.iterator().next)
    jedis.close()
    res
  }

  override def fetchLast(key: GenericRecord, timeFrom:Long, timeTo:Long, defaultV:GenericRecord): GenericRecord = {
    val jedis = jedisPool.getResource
    if (auth != null) jedis.auth(auth)
    val set = jedis.zrevrangeByScore(keyToBytes(key), timeTo, timeFrom)
    val res = if (set.isEmpty) defaultV else bytesToValue(set.iterator().next)
    jedis.close()
    res
  }

  @transient private lazy val emptyValue = AvroUtils.createEmpty(valueSchema)
  override def defaultValue: GenericRecord = emptyValue

  protected def keyToBytes(row: GenericRecord): Array[Byte] = {
    val outBuf = bufPool.borrowObject()
    //outBuf.reset()
    keyCoder.encode(row, outBuf)
    val res = outBuf.toByteArray
    bufPool .returnObject(outBuf)
    res
  }

  protected def valueToBytes(row: GenericRecord): Array[Byte] = {
    val outBuf = bufPool.borrowObject()
    //outBuf.reset()
    valueCoder.encode(row, outBuf)
    val res = outBuf.toByteArray
    bufPool .returnObject(outBuf)
    res
  }

  protected def bytesToValue(bytes: Array[Byte]): GenericRecord = {
    val inBuf = new ByteBufferInputStream
    inBuf.setData(bytes)
    valueCoder.decode(inBuf)
  }

  protected def valueToBytes(row: GenericRecord, ts:Long): Array[Byte] = {
    val outBuf = bufPool .borrowObject()
    outBuf.reset()
    for (i <- 56 to 0 by -8) {outBuf.write(((ts >> i) & 0xff).toInt)}
    valueCoder.encode(row, outBuf)
    val res = outBuf.toByteArray
    bufPool .returnObject(outBuf)
    res
  }

  protected def bytesToValueTS(bytes: Array[Byte]): (GenericRecord, Long) = {
    val inBuf = new ByteBufferInputStream
    inBuf.setData(bytes)
    if (bytes != null && bytes.length >= 8) {
      val ts = (bytes(0).toLong << 56) | (bytes(1).toLong << 48) | (bytes(2).toLong << 40) | (bytes(3).toLong << 32) |
               (bytes(4).toLong << 24) | (bytes(5).toLong << 16) | (bytes(6).toLong << 8) | bytes(7).toLong
      inBuf.skip(8)
      (valueCoder.decode(inBuf), ts)
    } else null
  }

  private def zpopmin(jedis:Jedis, key: Array[Byte]) = {
    val client = jedis.getClient
    client.sendCommand(ZPOPMIN, key)
    val reply = client.getBinaryMultiBulkReply
    if (reply.isEmpty) null
    else new Tuple(reply.get(0), BuilderFactory.DOUBLE.build(reply.get(1)))

  }

  object ZPOPMIN extends ProtocolCommand {
    override def getRaw: Array[Byte] = SafeEncoder.encode("ZPOPMIN")
  }
}

class ByteArrayOutputStreamFactory extends BasePooledObjectFactory[ByteArrayOutputStream] {
  override def create(): ByteArrayOutputStream = new ByteArrayOutputStream(1024)

  override def wrap(obj: ByteArrayOutputStream): PooledObject[ByteArrayOutputStream] =
    new DefaultPooledObject[ByteArrayOutputStream](obj)

  override def passivateObject(pooledObject: PooledObject[ByteArrayOutputStream]): Unit =
    pooledObject.getObject.reset()
}