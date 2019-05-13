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

import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fuseinfo.common.conf.{ConfUtils, JsonConfReader}
import com.fuseinfo.jets.util.{AvroUtils, CommandUtils, VarUtils}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.coders.{AvroCoder, KvCoder}
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.{Filter, MapElements, ParDo}
import org.apache.beam.sdk.values.{KV, PCollection}

import scala.collection.JavaConversions._
import scala.collection.mutable

object BeamFlowBuilder {
  val packagePrefix: String = getClass.getPackage.getName + "."
  val vars = new java.util.concurrent.ConcurrentHashMap[String, String]

  def main(args: Array[String]): Unit = {
    val opts = new java.util.HashMap[String, String]
    val restArgs = CommandUtils.parsingArgs(args, opts, vars)
    val confName = if (restArgs.nonEmpty) restArgs(0) else "BeamFlowBuilder.yaml"
    val confReader = new JsonConfReader(new YAMLFactory())
    val rootNode = confReader.readFromStream(getClass.getClassLoader.getResourceAsStream(confName), true)

    val envNode = rootNode.get("env")
    if (envNode != null && envNode.isArray) {
      envNode.elements.foreach{nd =>
        val ndName = nd.asText
        val envValue = System.getenv(ndName)
        if (envValue != null) vars.put(ndName, envValue)
      }
    }

    val varNode = rootNode.get("vars")
    if (varNode != null && varNode.isObject) {
      varNode.fields().foreach(entry => vars.put(entry.getKey, VarUtils.enrichString(entry.getValue.asText, vars)))
    }

    val dagNode = rootNode.get("DAG")
    val procStoreNode = rootNode.get("ProcessorStore")
    val procStoreMap = if (procStoreNode != null) {
      ConfUtils.jsonToMap(procStoreNode.asInstanceOf[ObjectNode])
    } else new java.util.HashMap[String, AnyRef]

    val optArgs = opts.map{case (key,str) => if (str == null) s"--$key" else s"--$key=$str"}.toSeq

    val options = PipelineOptionsFactory.fromArgs(optArgs: _*).withValidation.create()
    val pipeline = Pipeline.create(options)
    processDAG(dagNode.asInstanceOf[ObjectNode], pipeline, procStoreMap)
    pipeline.run()
  }

  private def processDAG(dagNode: ObjectNode, pipeline: Pipeline, procStoreMap:java.util.Map[String, AnyRef]): Unit = {
    val processorMap = mutable.Map.empty[String, (PCollection[KV[GenericRecord, GenericRecord]], Schema, Schema)]
    val storeDefMap = new java.util.HashMap[String, java.util.Map[String, AnyRef]]
    var lastCollection:PCollection[KV[GenericRecord, GenericRecord]] = null
    var lastKeySchema: Schema = null
    var lastValueSchema: Schema = null
    dagNode.fields().foreach { entry =>
      val stepName = entry.getKey
      entry.getValue match {
        case stepNode: ObjectNode if stepNode.has("__source") =>
          val sourceType = stepNode.get("__source").asText
          val paramNode = ConfUtils.jsonToMap(stepNode, vars)
          val processor = newSource(sourceType, paramNode)
          lastKeySchema = processor.getKeySchema
          lastValueSchema = processor.getValueSchema
          lastCollection = processor(pipeline, stepName)
            .setCoder(KvCoder.of(getAvroCoder(lastKeySchema), getAvroCoder(lastValueSchema)))
          processorMap.put(stepName, (lastCollection, lastKeySchema, lastValueSchema))

        case stepNode: ObjectNode if stepNode.has("__function") =>
          val funcType = stepNode.get("__function").asText
          val paramNode = ConfUtils.jsonToMap(stepNode, vars)
          val from = stepNode.get("__from")
          val (fCollection, fKeySchema, fValueSchema) =
            if (from == null) (lastCollection, lastKeySchema, lastValueSchema)
            else processorMap.getOrElse(from.asText, (lastCollection, lastKeySchema, lastValueSchema))
          val processor = newFunc(funcType, paramNode, fKeySchema, fValueSchema)
          processor match {
            case doFn: TransformerFn =>
              doFn.setStoreMap(storeDefMap)
              lastKeySchema = doFn.getKeySchema
              lastValueSchema = doFn.getValueSchema
              lastCollection = fCollection.apply(ParDo.of(doFn))
                .setCoder(KvCoder.of(getAvroCoder(lastKeySchema), getAvroCoder(lastValueSchema)))
              processorMap.put(stepName, (lastCollection, lastKeySchema, lastValueSchema))

            case mapFn: MapFn =>
              lastKeySchema = mapFn.getKeySchema
              lastValueSchema = mapFn.getValueSchema
              lastCollection = fCollection.apply(MapElements.via(mapFn))
                .setCoder(KvCoder.of(getAvroCoder(lastKeySchema), getAvroCoder(lastValueSchema)))
              processorMap.put(stepName, (lastCollection, lastKeySchema, lastValueSchema))

            case filterFn: FilterFn =>
              lastCollection = fCollection.apply(Filter.by(filterFn))
                .setCoder(KvCoder.of(getAvroCoder(lastKeySchema), getAvroCoder(lastValueSchema)))
              processorMap.put(stepName, (lastCollection, lastKeySchema, lastValueSchema))
          }

        case stepNode: ObjectNode if stepNode.has("__sink") =>
          val sinkType = stepNode.get("__sink").asText
          val paramNode = ConfUtils.jsonToMap(stepNode, vars)
          val from = stepNode.get("__from")
          val (fCollection, fKeySchema, fValueSchema) =
            if (from == null) (lastCollection, lastKeySchema, lastValueSchema)
            else processorMap.getOrElse(from.asText, (lastCollection, lastKeySchema, lastValueSchema))
          val processor = newSink(sinkType, paramNode, fKeySchema, fValueSchema)
          fCollection.apply(stepName, processor)

        case stepNode: ObjectNode if stepNode.has("__store") =>
          val storeType = stepNode.get("__store").asText
          storeType match {
            case "ProcessorStore" =>
              storeDefMap.put(stepName, procStoreMap)
            case "KeyValueStore" =>

            case _ =>
          }

        case _ => throw new IllegalArgumentException("Unknown node")
      }
    }
  }

  private def getAvroCoder(schema:Schema) =
    AvroCoder.of(if (schema == null) AvroUtils.emptySchema else schema)

  private def schemaToString(schema:Schema) = if (schema != null) schema.toString else null

  private def newSource(value:String, param:java.util.Map[String, AnyRef]) = {
    val clazz = try {
      Class.forName(value)
    } catch {
      case _:ClassNotFoundException => Class.forName(packagePrefix + "source." + value)
    }
    clazz.getDeclaredConstructor(classOf[java.util.Map[String, AnyRef]]).newInstance(param).asInstanceOf[SourceParser]
  }

  private def newFunc(value:String, param:java.util.Map[String, AnyRef], keySchema:Schema, valueSchema:Schema) = {
    val clazz = try {
      Class.forName(value)
    } catch {
      case _:ClassNotFoundException => Class.forName(packagePrefix + "function." + value)
    }
    clazz.getDeclaredConstructor(classOf[java.util.Map[String, AnyRef]], classOf[String], classOf[String])
      .newInstance(param, schemaToString(keySchema), valueSchema.toString)
  }

  private def newSink(value:String, param:java.util.Map[String, AnyRef], keySchema:Schema, valueSchema:Schema) = {
    val clazz = try {
      Class.forName(value)
    } catch {
      case _:ClassNotFoundException => Class.forName(packagePrefix + "sink." + value)
    }
    clazz.getDeclaredConstructor(classOf[java.util.Map[String, AnyRef]], classOf[String], classOf[String])
      .newInstance(param, schemaToString(keySchema), valueSchema.toString).asInstanceOf[SinkFormatter]
  }

}
