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

package com.fuseinfo.jets.kafka.function

import com.fasterxml.jackson.databind.ObjectMapper
import com.fuseinfo.jets.kafka.source.JsonData
import net.manub.embeddedkafka.EmbeddedKafka
import net.manub.embeddedkafka.schemaregistry.{EmbeddedKafkaConfigWithSchemaRegistry, EmbeddedKafkaWithSchemaRegistry}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.ForeachAction
import org.scalatest.FunSuite

import scala.collection.mutable

class ScalaKeyMapperSuite extends FunSuite with EmbeddedKafkaWithSchemaRegistry {
  test("test-scala-keyMapper-simple") {
    implicit val config: EmbeddedKafkaConfigWithSchemaRegistry = EmbeddedKafkaConfigWithSchemaRegistry(
      customBrokerProperties = Map("zookeeper.connection.timeout.ms" -> "60000"))
    withRunningKafka {
      val results = mutable.ArrayBuffer.empty[GenericRecord]
      scala.util.Try(EmbeddedKafka.createCustomTopic("unit-test"))
      val builder = new StreamsBuilder()
      val schemaIn = (new Schema.Parser).parse(getClass.getClassLoader.getResourceAsStream("event.avsc"))
      val mapper = new ObjectMapper()
      val objNode = mapper.createObjectNode()
      val rulesNode = mapper.createObjectNode()
      rulesNode.put("customerId", """event_data.getOrDefault("customer_id","0").toLong""")
      objNode.put("schema", """{"type":"record","name":"eventKey","fields":[{"name":"customerId","type":"long"}]}""")
      objNode.set("keyMapping", rulesNode)
      val streams = JsonData.getKafkaStreamWithData(builder, "unit-test") { stream =>
        val keyMapper = new ScalaKeyMapper(objNode, null, schemaIn)
        val transformed = stream.selectKey[GenericRecord](keyMapper)
        transformed.foreach(new ForeachAction[GenericRecord, GenericRecord]() {
          override def apply(key: GenericRecord, value: GenericRecord): Unit = {
            results += key
          }
        })
      }
      streams.start()
      val till = System.currentTimeMillis + 60000
      while (results.size < 10 && System.currentTimeMillis < till) {
        Thread.sleep(100)
      }
      streams.close()
      assert(results.size === 10)
      assert(results(0).toString === """{"customerId": 501013368151451}""")
      assert(results(1).toString === """{"customerId": 501013368151451}""")
      assert(results(2).toString === """{"customerId": 501015420060362}""")
      assert(results(3).toString === """{"customerId": 501015420060362}""")
      assert(results(4).toString === """{"customerId": 501013368151451}""")
      assert(results(5).toString === """{"customerId": 501011192685353}""")
      assert(results(6).toString === """{"customerId": 501011192685353}""")
      assert(results(7).toString === """{"customerId": 501011192685353}""")
      assert(results(8).toString === """{"customerId": 501011192685353}""")
      assert(results(9).toString === """{"customerId": 501013368151451}""")
    }
  }
}
