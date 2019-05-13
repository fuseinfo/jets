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

import com.fuseinfo.jets.beam.sink.TextSink
import com.fuseinfo.jets.beam.source.TextSource
import com.fuseinfo.jets.beam.util.FileUtils
import com.fuseinfo.jets.util.AvroUtils
import org.apache.avro.Schema
import org.apache.beam.runners.direct.DirectRunner
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.coders.{AvroCoder, KvCoder}
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.Filter
import org.scalatest.FunSuite

import scala.collection.mutable

class ScalaPredicateSuite extends FunSuite {
  test("beam-predicate") {
    val tmpDir = new java.io.File("tmp/beam-unit-predicate")
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

    val objNode = new java.util.LinkedHashMap[String, AnyRef]
    objNode.put("test", """event_data.getOrDefault("customer_id","0") == "501013368151451"""")
    val filterFn = new ScalaPredicate(objNode, null, kafkaSource.getValueSchema.toString)

    val sinkNode = new java.util.LinkedHashMap[String, AnyRef]
    sinkNode.put("path", "tmp/beam-unit-predicate/output.txt")
    val formatterNode = new java.util.LinkedHashMap[String, AnyRef]
    formatterNode.put("__formatter", "JsonFormatter")
    sinkNode.put("formatter", formatterNode)
    val textSink = new TextSink(sinkNode, null, kafkaSource.getValueSchema.toString)
    kafkaSource.apply(pipeline, "kafka_source")
      .apply(Filter.by(filterFn))
      .setCoder(KvCoder.of(getAvroCoder(filterFn.getKeySchema), getAvroCoder(filterFn.getValueSchema)))
      .apply(textSink)

    pipeline.run()
    val buffer = mutable.ArrayBuffer.empty[String]
    tmpDir.listFiles().foreach{file =>
      val source = scala.io.Source.fromFile(file)
      buffer ++= source.getLines
      source.close()
    }
    assert(buffer.size === 4)
    val results = buffer.sorted
    assert(results(0).toString === """{"event_timestamp": "2019-03-20T15:01:02.064-04:00", "event_source": "MDM", "event_type": "login", "event_data": {"customer_IP": "172.16.38.33", "customer_id": "501013368151451", "customer_name": "David Jone"}}""")
    assert(results(1).toString === """{"event_timestamp": "2019-03-20T15:01:05.102-04:00", "event_source": "MDM", "event_type": "getAccount", "event_data": {"customer_IP": "172.16.38.33", "customer_id": "501013368151451", "account": "394838505"}}""")
    assert(results(2).toString === """{"event_timestamp": "2019-03-20T15:01:42.430-04:00", "event_source": "MDM", "event_type": "getAddress", "event_data": {"country": "CANADA", "province": "ON", "address2": "APT 1510", "city": "TORONTO", "address1": "100 BLOOR ST", "customer_IP": "172.16.38.33", "postal": "M5P1N3", "customer_id": "501013368151451"}}""")
    assert(results(3).toString === """{"event_timestamp": "2019-03-20T15:02:59.900-04:00", "event_source": "MDM", "event_type": "updateAddress", "event_data": {"country": "CANADA", "province": "ON", "address2": "SUITE 2849", "city": "TORONTO", "address1": "33 BAY ST", "customer_IP": "172.16.38.33", "postal": "M5N2Z4", "customer_id": "501013368151451"}}""")
  }

  private def getAvroCoder(schema:Schema) = {
    AvroCoder.of(if (schema == null) AvroUtils.emptySchema else schema)
  }
}
