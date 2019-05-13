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

import net.manub.embeddedkafka.EmbeddedKafka
import net.manub.embeddedkafka.schemaregistry.{EmbeddedKafkaConfigWithSchemaRegistry, EmbeddedKafkaWithSchemaRegistry}
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.ForeachAction
import org.scalatest.FunSuite

class JsonSourceStreamSuite extends FunSuite with EmbeddedKafkaWithSchemaRegistry {

  test("test-json-source") {
    implicit val config:EmbeddedKafkaConfigWithSchemaRegistry = EmbeddedKafkaConfigWithSchemaRegistry(
      customBrokerProperties=Map("zookeeper.connection.timeout.ms"->"60000"))
    withRunningKafka {
      scala.util.Try(EmbeddedKafka.createCustomTopic("unit-test"))
      val results = new java.util.concurrent.ConcurrentLinkedQueue[GenericRecord]
      val builder = new StreamsBuilder()
      val streams = JsonData.getKafkaStreamWithData(builder, "unit-test"){stream =>
        stream.foreach(new ForeachAction[GenericRecord, GenericRecord]() {
          override def apply(key: GenericRecord, value: GenericRecord): Unit = {
            results.add(value)
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
      assertRec(results.poll, "2019-03-20T15:01:02.064-04:00", "MDM", "login",
        "501013368151451", "172.16.38.33", "David Jone", null,
        null, null, null, null, null, null)

      assertRec(results.poll, "2019-03-20T15:01:05.102-04:00", "MDM", "getAccount",
        "501013368151451", "172.16.38.33", null, "394838505",
        null, null, null, null, null, null)

      assertRec(results.poll, "2019-03-20T15:01:08.267-04:00", "MDM", "login",
        "501015420060362", "172.18.20.45", "John Smith", null,
        null, null, null, null, null, null)

      assertRec(results.poll, "2019-03-20T15:01:09.320-04:00", "MDM", "getAccount",
        "501015420060362", "172.18.20.45", null, "542006036",
        null, null, null, null, null, null)

      assertRec(results.poll, "2019-03-20T15:01:42.430-04:00", "MDM", "getAddress",
        "501013368151451", "172.16.38.33", null, null,
        "100 BLOOR ST", "APT 1510", "TORONTO", "ON", "CANADA", "M5P1N3")

      assertRec(results.poll, "2019-03-20T15:02:34.567-04:00", "MDM", "login",
        "501011192685353", "172.20.76.129", "Eva Bush", null,
        null, null, null, null, null, null)

      assertRec(results.poll, "2019-03-20T15:02:36.600-04:00", "MDM", "getAccount",
        "501011192685353", "172.20.76.129", null, "119268535",
        null, null, null, null, null, null)

      assertRec(results.poll, "2019-03-20T15:02:58.700-04:00", "MDM", "getAddress",
        "501011192685353", "172.20.76.129", null, null,
        "10 YONGE ST", "SUITE 3847", "TORONTO", "ON", "CANADA", "M1N2P3")

      assertRec(results.poll, "2019-03-20T15:02:58.800-04:00", "MDM", "updateAddress",
        "501011192685353", "172.20.76.129", null, null,
        "100 BLOOR ST", "APT 2802", "TORONTO", "ON", "CANADA", "M5P1N3")

      assertRec(results.poll, "2019-03-20T15:02:59.900-04:00", "MDM", "updateAddress",
        "501013368151451", "172.16.38.33", null, null,
        "33 BAY ST", "SUITE 2849", "TORONTO", "ON", "CANADA", "M5N2Z4")
    }
  }
  private def assertRec(record:GenericRecord, event_timestamp:String, event_source:String, event_type:String,
                        customer_id:String, customer_IP:String, customer_name:String, account:String, address1:String,
                        address2:String, city:String, province:String, country:String, postal: String): Unit = {
    assert(record.get("event_timestamp") === event_timestamp)
    assert(record.get("event_source") === event_source)
    assert(record.get("event_type") === event_type)
    assert(record.get("event_data").asInstanceOf[java.util.Map[String, String]].get("customer_id") === customer_id)
    assert(record.get("event_data").asInstanceOf[java.util.Map[String, String]].get("customer_IP") === customer_IP)
    assert(record.get("event_data").asInstanceOf[java.util.Map[String, String]].get("customer_name") === customer_name)
    assert(record.get("event_data").asInstanceOf[java.util.Map[String, String]].get("account") === account)
    assert(record.get("event_data").asInstanceOf[java.util.Map[String, String]].get("address1") === address1)
    assert(record.get("event_data").asInstanceOf[java.util.Map[String, String]].get("address2") === address2)
    assert(record.get("event_data").asInstanceOf[java.util.Map[String, String]].get("city") === city)
    assert(record.get("event_data").asInstanceOf[java.util.Map[String, String]].get("province") === province)
    assert(record.get("event_data").asInstanceOf[java.util.Map[String, String]].get("country") === country)
    assert(record.get("event_data").asInstanceOf[java.util.Map[String, String]].get("postal") === postal)
  }
}
