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

package com.fuseinfo.jets.beam.function

import com.fuseinfo.jets.beam.sink.KafkaSink
import com.fuseinfo.jets.beam.source.{JsonData, TextSource}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.beam.runners.direct.DirectRunner
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.coders.{AvroCoder, KvCoder}
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.MapElements
import org.scalatest.FunSuite

class ScalaKeyMapperSuite extends FunSuite with EmbeddedKafka {
  test("beam-key-mapper") {
    implicit val config: EmbeddedKafkaConfig =
        EmbeddedKafkaConfig(customBrokerProperties=Map("zookeeper.connection.timeout.ms"->"60000"))
    withRunningKafka {
      scala.util.Try(EmbeddedKafka.createCustomTopic("beam-unit-keymapper"))

      val options = PipelineOptionsFactory.fromArgs("--blockOnRun=false").withValidation().create()
      options.setRunner(classOf[DirectRunner])

      val pipeline = Pipeline.create(options)
      val sourceNode = new java.util.LinkedHashMap[String, AnyRef]
      sourceNode.put("path","src/test/resources/events.txt")
      val schema = scala.io.Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("event.avsc")).mkString
      sourceNode.put("valueSchema",schema)
      val parserNode = new java.util.LinkedHashMap[String, AnyRef]
      parserNode.put("__parser", "JsonParser")
      sourceNode.put("valueParser", parserNode)
      val kafkaSource = new TextSource(sourceNode)

      val objNode = new java.util.LinkedHashMap[String, AnyRef]
      val rulesNode = new java.util.LinkedHashMap[String, AnyRef]
      rulesNode.put("customerId", """event_data.getOrDefault("customer_id","0").toLong""")
      objNode.put("schema", """{"type":"record","name":"eventKey","fields":[{"name":"customerId","type":"long"}]}""")
      objNode.put("keyMapping", rulesNode)
      val keyMapperFn = new ScalaKeyMapper(objNode, null, kafkaSource.getValueSchema.toString)

      val sinkNode = new java.util.LinkedHashMap[String, AnyRef]
      sinkNode.put("bootstrap.servers","localhost:6001")
      sinkNode.put("topic", "beam-unit-keymapper")
      val formatterNode = new java.util.LinkedHashMap[String, AnyRef]
      formatterNode.put("__formatter", "JsonFormatter")
      sinkNode.put("formatter", formatterNode)
      val kafkaSink = new KafkaSink(sinkNode, keyMapperFn.getKeySchema.toString, kafkaSource.getValueSchema.toString)
      kafkaSource.apply(pipeline, "kafka_source")
        .apply(MapElements.via(keyMapperFn))
        .setCoder(KvCoder.of(AvroCoder.of(keyMapperFn.getKeySchema), AvroCoder.of(keyMapperFn.getValueSchema)))
        .apply(kafkaSink)

      val pipelineResult = pipeline.run()
      val results = JsonData.readDataKey("beam-unit-keymapper", 10, 60000).sorted
      assert(results.size === 10)
      assert(results(0) === """{"customerId": 501011192685353}""")
      assert(results(1) === """{"customerId": 501011192685353}""")
      assert(results(2) === """{"customerId": 501011192685353}""")
      assert(results(3) === """{"customerId": 501011192685353}""")
      assert(results(4) === """{"customerId": 501013368151451}""")
      assert(results(5) === """{"customerId": 501013368151451}""")
      assert(results(6) === """{"customerId": 501013368151451}""")
      assert(results(7) === """{"customerId": 501013368151451}""")
      assert(results(8) === """{"customerId": 501015420060362}""")
      assert(results(9) === """{"customerId": 501015420060362}""")
      pipelineResult.cancel()
    }
  }

}
