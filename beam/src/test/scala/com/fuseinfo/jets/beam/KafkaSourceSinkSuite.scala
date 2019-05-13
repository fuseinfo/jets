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

package com.fuseinfo.jets.beam

import com.fuseinfo.jets.beam.sink.KafkaSink
import com.fuseinfo.jets.beam.source.{JsonData, KafkaSource}
import com.fuseinfo.jets.util.AvroUtils
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.avro.Schema
import org.apache.beam.runners.direct.DirectRunner
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.coders.{AvroCoder, KvCoder}
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.scalatest.FunSuite

class KafkaSourceSinkSuite extends FunSuite with EmbeddedKafka {

  test("test kafka source and sink") {
    implicit val config: EmbeddedKafkaConfig =
      EmbeddedKafkaConfig(customBrokerProperties=Map("zookeeper.connection.timeout.ms"->"60000"))
    withRunningKafka {
      scala.util.Try(EmbeddedKafka.createCustomTopic("beam-unit-in"))
      scala.util.Try(EmbeddedKafka.createCustomTopic("beam-unit-out"))
      JsonData.loadData("beam-unit-in", "events.txt")
      val options = PipelineOptionsFactory.fromArgs("--blockOnRun=false").withValidation().create()
      options.setRunner(classOf[DirectRunner])

      val pipeline = Pipeline.create(options)
      val sourceNode = new java.util.LinkedHashMap[String, AnyRef]
      sourceNode.put("bootstrap.servers","localhost:6001")
      sourceNode.put("topic", "beam-unit-in")
      sourceNode.put("group.id", "KafkaSourceSuite")
      sourceNode.put("auto.offset.reset", "earliest")
      val schema = scala.io.Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("event.avsc")).mkString
      sourceNode.put("valueSchema",schema)
      val parserNode = new java.util.LinkedHashMap[String, AnyRef]
      parserNode.put("__parser", "JsonParser")
      sourceNode.put("valueParser", parserNode)
      val kafkaSource = new KafkaSource(sourceNode)

      val sinkNode = new java.util.LinkedHashMap[String, AnyRef]
      sinkNode.put("bootstrap.servers","localhost:6001")
      sinkNode.put("topic", "beam-unit-out")
      val formatterNode = new java.util.LinkedHashMap[String, AnyRef]
      formatterNode.put("__formatter", "JsonFormatter")
      sinkNode.put("valueFormatter", formatterNode)
      val kafkaSink = new KafkaSink(sinkNode, null, kafkaSource.getValueSchema.toString)
      kafkaSource.apply(pipeline, "kafka_source")
        .setCoder(KvCoder.of(getAvroCoder(kafkaSource.getKeySchema), getAvroCoder(kafkaSource.getValueSchema)))
        .apply(kafkaSink)

      val pipelineResult = pipeline.run()
      val results = JsonData.readData("beam-unit-out", 10, 60000).sorted
      assert(results.size === 10)
      assert(results(0) === """{"event_timestamp": "2019-03-20T15:01:02.064-04:00", "event_source": "MDM", "event_type": "login", "event_data": {"customer_IP": "172.16.38.33", "customer_id": "501013368151451", "customer_name": "David Jone"}}""")
      assert(results(1) === """{"event_timestamp": "2019-03-20T15:01:05.102-04:00", "event_source": "MDM", "event_type": "getAccount", "event_data": {"customer_IP": "172.16.38.33", "customer_id": "501013368151451", "account": "394838505"}}""")
      assert(results(2) === """{"event_timestamp": "2019-03-20T15:01:08.267-04:00", "event_source": "MDM", "event_type": "login", "event_data": {"customer_IP": "172.18.20.45", "customer_id": "501015420060362", "customer_name": "John Smith"}}""")
      assert(results(3) === """{"event_timestamp": "2019-03-20T15:01:09.320-04:00", "event_source": "MDM", "event_type": "getAccount", "event_data": {"customer_IP": "172.18.20.45", "customer_id": "501015420060362", "account": "542006036"}}""")
      assert(results(4) === """{"event_timestamp": "2019-03-20T15:01:42.430-04:00", "event_source": "MDM", "event_type": "getAddress", "event_data": {"country": "CANADA", "province": "ON", "address2": "APT 1510", "city": "TORONTO", "address1": "100 BLOOR ST", "customer_IP": "172.16.38.33", "postal": "M5P1N3", "customer_id": "501013368151451"}}""")
      assert(results(5) === """{"event_timestamp": "2019-03-20T15:02:34.567-04:00", "event_source": "MDM", "event_type": "login", "event_data": {"customer_IP": "172.20.76.129", "customer_id": "501011192685353", "customer_name": "Eva Bush"}}""")
      assert(results(6) === """{"event_timestamp": "2019-03-20T15:02:36.600-04:00", "event_source": "MDM", "event_type": "getAccount", "event_data": {"customer_IP": "172.20.76.129", "customer_id": "501011192685353", "account": "119268535"}}""")
      assert(results(7) === """{"event_timestamp": "2019-03-20T15:02:58.700-04:00", "event_source": "MDM", "event_type": "getAddress", "event_data": {"country": "CANADA", "province": "ON", "address2": "SUITE 3847", "city": "TORONTO", "address1": "10 YONGE ST", "customer_IP": "172.20.76.129", "postal": "M1N2P3", "customer_id": "501011192685353"}}""")
      assert(results(8) === """{"event_timestamp": "2019-03-20T15:02:58.800-04:00", "event_source": "MDM", "event_type": "updateAddress", "event_data": {"country": "CANADA", "province": "ON", "address2": "APT 2802", "city": "TORONTO", "address1": "100 BLOOR ST", "customer_IP": "172.20.76.129", "postal": "M5P1N3", "customer_id": "501011192685353"}}""")
      assert(results(9) === """{"event_timestamp": "2019-03-20T15:02:59.900-04:00", "event_source": "MDM", "event_type": "updateAddress", "event_data": {"country": "CANADA", "province": "ON", "address2": "SUITE 2849", "city": "TORONTO", "address1": "33 BAY ST", "customer_IP": "172.16.38.33", "postal": "M5N2Z4", "customer_id": "501013368151451"}}""")
      pipelineResult.cancel()
    }
  }

  private def getAvroCoder(schema:Schema) = {
    AvroCoder.of(if (schema == null) AvroUtils.emptySchema else schema)
  }
}
