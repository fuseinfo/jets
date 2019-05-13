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

package com.fuseinfo.jets.kafka.source

import java.util
import java.util.Properties
import java.util.regex.Pattern

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder}

import scala.collection.JavaConversions._
import scala.collection.mutable

object JsonData {
  def loadData(topic:String, fileName:String): Unit = {
    val props = new Properties()
    props.setProperty("bootstrap.servers","localhost:6001")
    props.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
    props.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer")
    Thread.sleep(1000)
    val producer = new KafkaProducer[String, String](props)
    scala.io.Source.fromInputStream(getClass.getClassLoader.getResourceAsStream(fileName), "UTF-8").getLines
      .foreach {msg => producer.send(new ProducerRecord[String, String](topic, null, msg))}
    producer.close()
  }

  def loadRealTimeData(topic:String, fileName:String): Unit = {
    val regex = Pattern.compile("\"event_timestamp\":\"[^\"]+\\.(\\d{3})[^\"]+\"")
    val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
    val props = new Properties()
    props.setProperty("bootstrap.servers","localhost:6001")
    props.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
    props.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer")
    Thread.sleep(1000)
    var time = (System.currentTimeMillis / 1000 - 1) * 1000
    val producer = new KafkaProducer[String, String](props)
    scala.io.Source.fromInputStream(getClass.getClassLoader.getResourceAsStream(fileName), "UTF-8").getLines
      .foreach {msg =>
        val matcher = regex.matcher(msg)
        matcher.find()
        val millis = matcher.group(1).toInt
        val newmsg = regex.matcher(msg).replaceFirst("\"event_timestamp\":\"" + sdf.format(new java.util.Date(time + millis)) + "\"")
        producer.send(new ProducerRecord[String, String](topic, null, newmsg))}
    producer.close()
  }

  def readData(topic:String, max:Int, timeout:Long): mutable.ArrayBuffer[String] = {
    val results = new mutable.ArrayBuffer[String](max)
    val props = new Properties()
    props.setProperty("bootstrap.servers","localhost:6001")
    props.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("group.id", "test")
    props.setProperty("enable.auto.commit","true")
    props.setProperty("auto.offset.reset", "earliest")
    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(util.Arrays.asList(topic))
    val till = System.currentTimeMillis + timeout
    while (results.size < max && System.currentTimeMillis < till) {
      val records = consumer.poll(100)
      records.foreach{record =>
        System.out.println(record.value)
        results += record.value}
    }
    consumer.close()
    results
  }

  def getKafkaStreamWithData(builder: StreamsBuilder, topic:String)(f: KStream[GenericRecord, GenericRecord] => Unit): KafkaStreams = {
    val props = new Properties()
    props.setProperty("application.id","JsonSourceStreamSuite")
    props.setProperty("bootstrap.servers","localhost:6001")
    props.setProperty("client.id","JsonSourceStreamSuite")
    props.setProperty("default.key.serde","org.apache.kafka.common.serialization.Serdes$ByteArraySerde")
    props.setProperty("default.value.serde","org.apache.kafka.common.serialization.Serdes$ByteArraySerde")
    props.setProperty("schema.registry.url","http://localhost:6002")
    loadData(topic, "events.txt")
    val mapper = new ObjectMapper()
    val objNode = mapper.createObjectNode()
    objNode.put("topic", topic)
    val schema = scala.io.Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("event.avsc")).mkString
    objNode.put("valueSchema", schema)
    val parserNode = mapper.createObjectNode()
    parserNode.put("__parser", "JsonParser")
    objNode.set("valueParser", parserNode)
    val source = new KafkaSource(objNode)
    f(source.apply(builder))
    new KafkaStreams(builder.build(), props)
  }
}