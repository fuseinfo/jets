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

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fuseinfo.common.conf.ConfUtils
import com.fuseinfo.jets.beam.sink.TextSink
import com.fuseinfo.jets.beam.source.TextSource
import com.fuseinfo.jets.beam.util.FileUtils
import com.fuseinfo.jets.util.AvroUtils
import org.apache.avro.Schema
import org.apache.beam.runners.direct.DirectRunner
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.coders.{AvroCoder, KvCoder}
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.ParDo
import org.scalatest.FunSuite

import scala.collection.mutable

class ScalaTransformerSuite extends FunSuite {

  test("beam-transform") {
    val tmpDir = new java.io.File("tmp/beam-unit-transform")
    FileUtils.deleteRecursively(tmpDir)
    scala.util.Try(tmpDir.mkdirs())

    val options = PipelineOptionsFactory.fromArgs("--blockOnRun=true").withValidation().create()
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

    val transNode = new java.util.LinkedHashMap[String, AnyRef]
    val mapperYaml = new ObjectMapper(new YAMLFactory())
    val schemaOut = scala.io.Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("eventFmt.avsc")).mkString
    val rulesNode = mapperYaml.readTree(getClass.getClassLoader.getResourceAsStream("event.yaml"))
    val rulesMap = ConfUtils.jsonToMap(rulesNode.asInstanceOf[ObjectNode])
    transNode.put("schema", schemaOut)
    transNode.put("valueMapping", rulesMap)
    val transformFn = new ScalaTransformer(transNode, null, kafkaSource.getValueSchema.toString)

    val sinkNode = new java.util.LinkedHashMap[String, AnyRef]
    sinkNode.put("path", "tmp/beam-unit-transform/output.txt")
    val formatterNode = new java.util.LinkedHashMap[String, AnyRef]
    formatterNode.put("__formatter", "JsonFormatter")
    sinkNode.put("formatter", formatterNode)
    val textSink = new TextSink(sinkNode, null, transformFn.getValueSchema.toString)
    kafkaSource.apply(pipeline, "kafka_source")
      .apply(ParDo.of(transformFn))
      .setCoder(KvCoder.of(getAvroCoder(transformFn.getKeySchema), getAvroCoder(transformFn.getValueSchema)))
      .apply(textSink)

    pipeline.run()
    val buffer = mutable.ArrayBuffer.empty[String]
    tmpDir.listFiles().foreach{file =>
      val source = scala.io.Source.fromFile(file)
      buffer ++= source.getLines
      source.close()
    }
    assert(buffer.size === 10)
    val results = buffer.sorted
    assert(results(0) === """{"event_timestamp": 1553108462064, "customer_id": 0, "event_type": "login", "event_common": {"event_IP": "172.16.38.33", "event_source": "MDM"}, "event_data": {"customer_name": "David Jone"}}""")
    assert(results(1) === """{"event_timestamp": 1553108465102, "customer_id": 0, "event_type": "getAccount", "event_common": {"event_IP": "172.16.38.33", "event_source": "MDM"}, "event_data": {"account": "394838505"}}""")
    assert(results(2) === """{"event_timestamp": 1553108468267, "customer_id": 0, "event_type": "login", "event_common": {"event_IP": "172.18.20.45", "event_source": "MDM"}, "event_data": {"customer_name": "John Smith"}}""")
    assert(results(3) === """{"event_timestamp": 1553108469320, "customer_id": 0, "event_type": "getAccount", "event_common": {"event_IP": "172.18.20.45", "event_source": "MDM"}, "event_data": {"account": "542006036"}}""")
    assert(results(4) === """{"event_timestamp": 1553108502430, "customer_id": 0, "event_type": "getAddress", "event_common": {"event_IP": "172.16.38.33", "event_source": "MDM"}, "event_data": {"country": "CANADA", "postal": "M5P1N3", "province": "ON", "city": "TORONTO", "address2": "APT 1510", "address1": "100 BLOOR ST"}}""")
    assert(results(5) === """{"event_timestamp": 1553108554567, "customer_id": 0, "event_type": "login", "event_common": {"event_IP": "172.20.76.129", "event_source": "MDM"}, "event_data": {"customer_name": "Eva Bush"}}""")
    assert(results(6) === """{"event_timestamp": 1553108556600, "customer_id": 0, "event_type": "getAccount", "event_common": {"event_IP": "172.20.76.129", "event_source": "MDM"}, "event_data": {"account": "119268535"}}""")
    assert(results(7) === """{"event_timestamp": 1553108578700, "customer_id": 0, "event_type": "getAddress", "event_common": {"event_IP": "172.20.76.129", "event_source": "MDM"}, "event_data": {"country": "CANADA", "postal": "M1N2P3", "province": "ON", "city": "TORONTO", "address2": "SUITE 3847", "address1": "10 YONGE ST"}}""")
    assert(results(8) === """{"event_timestamp": 1553108578800, "customer_id": 0, "event_type": "updateAddress", "event_common": {"event_IP": "172.20.76.129", "event_source": "MDM"}, "event_data": {"country": "CANADA", "postal": "M5P1N3", "province": "ON", "city": "TORONTO", "address2": "APT 2802", "address1": "100 BLOOR ST"}}""")
    assert(results(9) === """{"event_timestamp": 1553108579900, "customer_id": 0, "event_type": "updateAddress", "event_common": {"event_IP": "172.16.38.33", "event_source": "MDM"}, "event_data": {"country": "CANADA", "postal": "M5N2Z4", "province": "ON", "city": "TORONTO", "address2": "SUITE 2849", "address1": "33 BAY ST"}}""")

  }

  private def getAvroCoder(schema:Schema) = {
    AvroCoder.of(if (schema == null) AvroUtils.emptySchema else schema)
  }
}
