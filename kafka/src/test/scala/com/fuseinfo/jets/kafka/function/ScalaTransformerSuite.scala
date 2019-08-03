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
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fuseinfo.jets.kafka.source.JsonData
import net.manub.embeddedkafka.EmbeddedKafka
import net.manub.embeddedkafka.schemaregistry.{EmbeddedKafkaConfigWithSchemaRegistry, EmbeddedKafkaWithSchemaRegistry}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.ForeachAction
import org.scalatest.FunSuite

import scala.collection.mutable

class ScalaTransformerSuite extends FunSuite with EmbeddedKafkaWithSchemaRegistry {

  test("test-scala-transform-simple") {
    implicit val config:EmbeddedKafkaConfigWithSchemaRegistry = EmbeddedKafkaConfigWithSchemaRegistry(
      customBrokerProperties=Map("zookeeper.connection.timeout.ms"->"60000"))
    withRunningKafka {
      val results = mutable.ArrayBuffer.empty[GenericRecord]
      scala.util.Try(EmbeddedKafka.createCustomTopic("unit-test"))
      val builder = new StreamsBuilder()
      val schemaIn = (new Schema.Parser).parse(getClass.getClassLoader.getResourceAsStream("event.avsc"))
      val mapper = new ObjectMapper()
      val mapperYaml = new ObjectMapper(new YAMLFactory())
      val objNode = mapper.createObjectNode()
      val schemaOut = scala.io.Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("eventFmt.avsc")).mkString
      val rulesNode = mapperYaml.readTree(getClass.getClassLoader.getResourceAsStream("event.yaml"))
      objNode.put("schema", schemaOut)
      objNode.set("valueMapping", rulesNode)
      val streams = JsonData.getKafkaStreamWithData(builder, "unit-test") { stream =>
        val transformer = new ScalaTransformer("unit", objNode,null, schemaIn)
        val transformed = stream.transform(transformer)
        transformed.foreach(new ForeachAction[GenericRecord, GenericRecord](){
          override def apply(key: GenericRecord, value: GenericRecord): Unit = {
            results += value
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
      assert(results(0).toString === """{"event_timestamp": 1553108462064, "customer_id": 501013368151451, "event_type": "login", "event_common": {"event_IP": "172.16.38.33", "event_source": "MDM"}, "event_data": {"customer_name": "David Jone"}}""")
      assert(results(1).toString === """{"event_timestamp": 1553108465102, "customer_id": 501013368151451, "event_type": "getAccount", "event_common": {"event_IP": "172.16.38.33", "event_source": "MDM"}, "event_data": {"account": "394838505"}}""")
      assert(results(2).toString === """{"event_timestamp": 1553108468267, "customer_id": 501015420060362, "event_type": "login", "event_common": {"event_IP": "172.18.20.45", "event_source": "MDM"}, "event_data": {"customer_name": "John Smith"}}""")
      assert(results(3).toString === """{"event_timestamp": 1553108469320, "customer_id": 501015420060362, "event_type": "getAccount", "event_common": {"event_IP": "172.18.20.45", "event_source": "MDM"}, "event_data": {"account": "542006036"}}""")
      assert(results(4).toString === """{"event_timestamp": 1553108502430, "customer_id": 501013368151451, "event_type": "getAddress", "event_common": {"event_IP": "172.16.38.33", "event_source": "MDM"}, "event_data": {"city": "TORONTO", "country": "CANADA", "province": "ON", "postal": "M5P1N3", "address2": "APT 1510", "address1": "100 BLOOR ST"}}""")
      assert(results(5).toString === """{"event_timestamp": 1553108554567, "customer_id": 501011192685353, "event_type": "login", "event_common": {"event_IP": "172.20.76.129", "event_source": "MDM"}, "event_data": {"customer_name": "Eva Bush"}}""")
      assert(results(6).toString === """{"event_timestamp": 1553108556600, "customer_id": 501011192685353, "event_type": "getAccount", "event_common": {"event_IP": "172.20.76.129", "event_source": "MDM"}, "event_data": {"account": "119268535"}}""")
      assert(results(7).toString === """{"event_timestamp": 1553108578700, "customer_id": 501011192685353, "event_type": "getAddress", "event_common": {"event_IP": "172.20.76.129", "event_source": "MDM"}, "event_data": {"city": "TORONTO", "country": "CANADA", "province": "ON", "postal": "M1N2P3", "address2": "SUITE 3847", "address1": "10 YONGE ST"}}""")
      assert(results(8).toString === """{"event_timestamp": 1553108578800, "customer_id": 501011192685353, "event_type": "updateAddress", "event_common": {"event_IP": "172.20.76.129", "event_source": "MDM"}, "event_data": {"city": "TORONTO", "country": "CANADA", "province": "ON", "postal": "M5P1N3", "address2": "APT 2802", "address1": "100 BLOOR ST"}}""")
      assert(results(9).toString === """{"event_timestamp": 1553108579900, "customer_id": 501013368151451, "event_type": "updateAddress", "event_common": {"event_IP": "172.16.38.33", "event_source": "MDM"}, "event_data": {"city": "TORONTO", "country": "CANADA", "province": "ON", "postal": "M5N2Z4", "address2": "SUITE 2849", "address1": "33 BAY ST"}}""")
    }
  }
}
