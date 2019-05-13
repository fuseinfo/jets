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

class ScalaPredicateSuite extends FunSuite with EmbeddedKafkaWithSchemaRegistry {

  test("test-scala-predicate-simple") {
    implicit val config:EmbeddedKafkaConfigWithSchemaRegistry = EmbeddedKafkaConfigWithSchemaRegistry(
      customBrokerProperties=Map("zookeeper.connection.timeout.ms"->"60000"))
    withRunningKafka {
      val results = mutable.ArrayBuffer.empty[GenericRecord]
      scala.util.Try(EmbeddedKafka.createCustomTopic("unit-test"))
      val builder = new StreamsBuilder()
      val schemaIn = (new Schema.Parser).parse(getClass.getClassLoader.getResourceAsStream("event.avsc"))
      val mapper = new ObjectMapper()
      val objNode = mapper.createObjectNode()
      objNode.put("test", """event_data.getOrDefault("customer_id","0") == "501013368151451"""")
      val streams = JsonData.getKafkaStreamWithData(builder, "unit-test") { stream =>
        val predicator = new ScalaPredicate(objNode,null, schemaIn)
        val transformed = stream.filter(predicator)
        transformed.foreach(new ForeachAction[GenericRecord, GenericRecord](){
          override def apply(key: GenericRecord, value: GenericRecord): Unit = {
            results += value
          }
        })
      }
      streams.start()
      val till = System.currentTimeMillis + 60000
      while (results.size < 4 && System.currentTimeMillis < till) {
        Thread.sleep(100)
      }
      streams.close()
      assert(results.size === 4)
      assert(results(0).toString === """{"event_timestamp": "2019-03-20T15:01:02.064-04:00", "event_source": "MDM", "event_type": "login", "event_data": {"customer_IP": "172.16.38.33", "customer_name": "David Jone", "customer_id": "501013368151451"}}""")
      assert(results(1).toString === """{"event_timestamp": "2019-03-20T15:01:05.102-04:00", "event_source": "MDM", "event_type": "getAccount", "event_data": {"customer_IP": "172.16.38.33", "customer_id": "501013368151451", "account": "394838505"}}""")
      assert(results(2).toString === """{"event_timestamp": "2019-03-20T15:01:42.430-04:00", "event_source": "MDM", "event_type": "getAddress", "event_data": {"country": "CANADA", "province": "ON", "address2": "APT 1510", "city": "TORONTO", "address1": "100 BLOOR ST", "customer_IP": "172.16.38.33", "postal": "M5P1N3", "customer_id": "501013368151451"}}""")
      assert(results(3).toString === """{"event_timestamp": "2019-03-20T15:02:59.900-04:00", "event_source": "MDM", "event_type": "updateAddress", "event_data": {"country": "CANADA", "province": "ON", "address2": "SUITE 2849", "city": "TORONTO", "address1": "33 BAY ST", "customer_IP": "172.16.38.33", "postal": "M5N2Z4", "customer_id": "501013368151451"}}""")
    }
  }
}
