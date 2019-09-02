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

package com.fuseinfo.jets.kafka

import java.io.{File, FileInputStream}
import java.util.Properties

import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode, TextNode}
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fuseinfo.common.conf.JsonConfReader
import com.fuseinfo.jets.kafka.util.{AvroSerde, JsonUtils}
import com.fuseinfo.jets.kafka.web.{DashboardClient, Edge, Node}
import com.fuseinfo.jets.util.{CommandUtils, VarUtils}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.Serdes._
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder}
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.state.Stores

import scala.collection.JavaConversions._
import scala.collection.mutable

object KafkaFlowBuilder {
  val packagePrefix: String = getClass.getPackage.getName + "."
  val vars = new java.util.concurrent.ConcurrentHashMap[String, String]

  def main(args: Array[String]): Unit = {
    val opts = new java.util.HashMap[String, String]
    val restArgs = CommandUtils.parsingArgs(args, opts, vars)
    val confName = if (restArgs.nonEmpty) restArgs(0) else "KafkaFlowBuilder.yaml"
    val confFile = new File(confName)
    val is = if (scala.util.Try(confFile.canRead).getOrElse(false)) new FileInputStream(new File(confName))
             else getClass.getClassLoader.getResourceAsStream(confName)
    val confReader = new JsonConfReader(new YAMLFactory())
    val rootNode = confReader.readFromStream(is, true)

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
    val (streams, nodes) = dagNode match {
      case dag:ObjectNode => processDAG(dag)
      case _ => throw new IllegalArgumentException("Missing DAG")
    }
    streams.foreach(_.start())

    rootNode.get("dashboard") match {
      case dashboardNode:ObjectNode =>
        val url = JsonUtils.getOrElse(dashboardNode, "url", "ws://127.0.0.1:8080")
        val appName = VarUtils.enrichString(JsonUtils.getOrElse(dashboardNode, "appName", "demo"), vars)
        val dashboardThread = new Thread {
          override def run(): Unit = {
            DashboardClient.heartbeatServices(url, appName, nodes, dagNode.asInstanceOf[ObjectNode])
          }
        }
        dashboardThread.setDaemon(true)
        dashboardThread.start()
      case _ =>
    }
  }

  private def processDAG(dagNode: ObjectNode) = {
    val builderMap = mutable.Map.empty[(Set[String], String), (StreamsBuilder, Properties)]
    val processorMap = mutable.Map.empty[String, (KStream[GenericRecord, GenericRecord], Schema, Schema, Node)]
    val storeDefMap = mutable.Map.empty[String, String]
    var lastBuilder:StreamsBuilder = null
    var lastKStream:KStream[GenericRecord, GenericRecord] = null
    var lastKeySchema:Schema = null
    var lastValueSchema:Schema = null
    var lastNode:Node = null

    dagNode.fields().foreach{entry =>
      val stepName = entry.getKey
      entry.getValue match {
        case stepNode:ObjectNode if stepNode.has("__source") =>
          val paramNode = stepNode.deepCopy()
          val servers = paramNode.get("bootstrap.servers") match {
            case serverList: TextNode =>
              VarUtils.enrichString(serverList.asText, vars).split(",").map(_.trim.toLowerCase).toSet
            case array: ArrayNode => array.elements().map(elem => VarUtils.enrichString(elem.asText, vars).trim).toSet
          }
          val id = VarUtils.enrichString(paramNode.get("group.id").asText, vars)
          val (builder, _) = builderMap.getOrElseUpdate((servers, id), (new StreamsBuilder, {
            val props = new Properties
            props.setProperty("application.id", id)
            props.setProperty("bootstrap.servers", servers.mkString(","))
            paramNode.fields().filter(_.getValue.isValueNode).foreach{entry =>
              val key = entry.getKey
              if (key != "group.id" && key != "topic") {
                props.setProperty(key, VarUtils.enrichString(entry.getValue.asText, vars))
              }
            }
            props
          }))
          lastBuilder = builder
          val sourceProcessor = JsonUtils.initSourceFunc(stepName, paramNode)
          lastKStream = sourceProcessor(builder)
          lastKeySchema = sourceProcessor.getKeySchema
          lastValueSchema = sourceProcessor.getValueSchema
          lastNode = new Node(stepName, sourceProcessor)
          processorMap.put(stepName, (lastKStream, lastKeySchema, lastValueSchema, lastNode))

        case stepNode:ObjectNode if stepNode.has("__function") =>
          val paramNode = stepNode.deepCopy()
          val from = paramNode.get("__from")
          val (fKStream, fKeySchema, fValueSchema, fNode, idx) =
            if (from == null) (lastKStream, lastKeySchema, lastValueSchema, lastNode, 0)
            else {
              val fromText = from.asText
              val dot = fromText.lastIndexOf('.')
              val (fromName, fromIdx) = if (dot <= 0) (fromText, 0)
                                        else (fromText.substring(0, dot), fromText.substring(dot + 1).toInt)
              val tuple = processorMap.getOrElse(fromName, (lastKStream, lastKeySchema, lastValueSchema, lastNode))
              (tuple._1, tuple._2, tuple._3, tuple._4, fromIdx)
            }
          val processor = JsonUtils.initProcessorFunc(stepName, paramNode, fKeySchema, fValueSchema)
          lastNode = new Node(stepName, processor)
          val edge = new Edge(fNode.nodeName + ":" + stepName, fNode, lastNode)
          lastNode.addInEdge(edge)
          fNode.addOutEdge(edge, idx)
          processor match {
            case fn: SchemaTransformerSupplier =>
              lastKStream = paramNode.get("stores") match {
                case null => fKStream.transform(fn)
                case storesNode =>
                  fn.setStoreMap(storeDefMap.toMap)
                  fKStream.transform(fn, storesNode.asText.split(",").map(_.trim):_*)
              }
              lastKeySchema = fKeySchema
              lastValueSchema = fn.getValueSchema
            case fn: AvroPredicate =>
              lastKStream = fKStream.filter(fn)
              lastKeySchema = fKeySchema
              lastValueSchema = fValueSchema
            case fn: KeyMapper =>
              lastKStream = fKStream.selectKey(fn)
              val through =  paramNode.get("__through")
              if (through != null) lastKStream = lastKStream.through(through.asText)
              lastKeySchema = fn.getKeySchema
              lastValueSchema = fValueSchema
            case fn: BranchPredicates =>
              val streams = fKStream.branch(fn:_*)
              streams.zipWithIndex.foreach { case (ks, j) =>
                processorMap.put(stepName + ":" + j, (ks, fKeySchema, fValueSchema, lastNode))
              }
              lastKStream = streams(0)
              lastKeySchema = fKeySchema
              lastValueSchema = fValueSchema
            case fn: SplitMapper =>
              val streamsMap = fn(fKStream)
              streamsMap.foreach(p => processorMap.put(p._1, (p._2, fKeySchema, fn.getValueSchema(p._1), lastNode)))
              lastKStream = streamsMap.head._2
              lastKeySchema = fKeySchema
              lastValueSchema = fn.getValueSchema(streamsMap.head._1)
            case fn: FlatKeyValueMapper =>
              lastKStream = fKStream.flatMap(fn)
              lastKeySchema = fn.getKeySchema
              lastValueSchema = fn.getValueSchema
            case fn: FlatValueMapper =>
              lastKStream = fKStream.flatMapValues(fn)
              lastKeySchema = fKeySchema
              lastValueSchema = fn.getValueSchema
            case fn: SingleKeyValueMapper =>
              lastKStream = fKStream.map(fn)
              lastKeySchema = fn.getKeySchema
              lastValueSchema = fn.getValueSchema
            case fn: SingleValueMapper =>
              lastKStream = fKStream.mapValues(fn)
              lastKeySchema = fKeySchema
              lastValueSchema = fn.getValueSchema
            case fn => throw new IllegalArgumentException("Unknown processor class: " + fn.getClass.getName)
          }
          processorMap.put(stepName, (lastKStream, lastKeySchema, lastValueSchema, lastNode))

        case stepNode:ObjectNode if stepNode.has("__sink") =>
          val paramNode = stepNode.deepCopy()
          val from = paramNode.get("__from")
          val (fKStream, fKeySchema, fValueSchema, fNode, idx) =
            if (from == null) (lastKStream, lastKeySchema, lastValueSchema, lastNode, 0)
            else {
              val fromText = from.asText
              val dot = fromText.lastIndexOf('.')
              val (fromName, fromIdx) = if (dot <= 0) (fromText, 0)
              else (fromText.substring(0, dot), fromText.substring(dot + 1).toInt)
              val tuple = processorMap.getOrElse(fromName, (lastKStream, lastKeySchema, lastValueSchema, lastNode))
              (tuple._1, tuple._2, tuple._3, tuple._4, fromIdx)
            }
          val sink = JsonUtils.initSinkFunc(stepName, paramNode, fKeySchema, fValueSchema)
          sink(fKStream)
          val sinkNode = new Node(stepName, sink)
          val edge = new Edge(fNode.nodeName + ":" + stepName, fNode, sinkNode)
          sinkNode.addInEdge(edge)
          fNode.addOutEdge(edge, idx)
          processorMap.put(stepName, (null, null, null, sinkNode))

        case stepNode:ObjectNode if stepNode.has("__store") =>
          val opts = stepNode.fields().filter(_.getValue.isValueNode).map(p => p.getKey -> p.getValue.asText).toMap
          val keyType = stepNode.get("keyType") match {
            case null => "GenericRecord"
            case jsonNode => jsonNode.asText
          }
          val keySerde = getSerde(keyType, lastKeySchema)
          val valueType = stepNode.get("valueType") match {
            case null => "GenericRecord"
            case jsonNode => jsonNode.asText
          }
          val valueSerde =  getSerde(valueType, lastValueSchema)
          val storeBuilder = stepNode.get("__store").asText match {
            case "InMemoryKeyValueStore" =>
              storeDefMap.put(stepName, s"org.apache.kafka.streams.state.KeyValueStore[$keyType,$valueType]")
              Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore(stepName), keySerde, valueSerde)
            case "RocksDBStore" =>
              storeDefMap.put(stepName, s"org.apache.kafka.streams.state.KeyValueStore[$keyType,$valueType]")
              Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(stepName), keySerde, valueSerde)
                .withLoggingEnabled(new java.util.HashMap[String, String])
                .withCachingEnabled()
            case "SessionStore" =>
              storeDefMap.put(stepName, s"org.apache.kafka.streams.state.SessionStore[$keyType,$valueType]")
              val retentionPeriod = opts.get("retentionPeriod").map(_.toLong).getOrElse(3600000L)
              Stores.sessionStoreBuilder(Stores.persistentSessionStore(stepName, retentionPeriod), keySerde, valueSerde)
            case "WindowStore" =>
              storeDefMap.put(stepName, s"org.apache.kafka.streams.state.WindowStore[$keyType,$valueType]")
              val retentionPeriod = opts.get("retentionPeriod").map(_.toLong).getOrElse(86400000L)
              val numSegments = opts.get("numSegments").map(_.toInt).getOrElse(3)
              val windowSize = opts.get("windowSize").map(_.toLong).getOrElse(3600000L)
              val retainDuplicates = opts.get("retainDuplicates").forall(_.toBoolean)
              Stores.windowStoreBuilder(Stores.persistentWindowStore(stepName, retentionPeriod, numSegments,
                windowSize, retainDuplicates), keySerde, valueSerde)
                .withLoggingEnabled(new java.util.HashMap[String, String])
                .withCachingEnabled()
            case "ProcessorStore" =>
              storeDefMap.put(stepName, s"com.fuseinfo.jets.kafka.store.ProcessorStore")
              val retentionPeriod = opts.get("retentionPeriod").map(_.toLong).getOrElse(86400000L)
              val numSegments = opts.get("numSegments").map(_.toInt).getOrElse(25)
              val windowSize = opts.get("windowSize").map(_.toLong).getOrElse(3600000L)
              Stores.windowStoreBuilder(Stores.persistentWindowStore(stepName, retentionPeriod, numSegments,
                windowSize, false), keySerde, valueSerde)
                .withLoggingEnabled(new java.util.HashMap[String, String])
            case _ => null
          }
          if (storeBuilder != null) {
            lastBuilder.addStateStore(storeBuilder)
          }

        case _ => throw new IllegalArgumentException("Unknown node")
      }
    }
    val streamList = builderMap.map{case (_,(builder, conf)) => new KafkaStreams(builder.build(), conf)}.toList
    val nodeList = processorMap.map(_._2._4).toList
    (streamList, nodeList)
  }

  private def getSerde(str: String, schema:Schema) = {
    str match {
      case "String" => new StringSerde()
      case "Short" => new ShortSerde()
      case "Integer" => new IntegerSerde()
      case "Long" => new LongSerde()
      case "Float" => new FloatSerde()
      case "Double" => new DoubleSerde()
      case "ByteArray" => new ByteArraySerde()
      case "ByteBuffer" => new ByteBufferSerde()
      case "Bytes" => new BytesSerde()
      case "GenericRecord" => new AvroSerde(schema)
      case _ => throw new IllegalArgumentException("Unknown data type")
    }
  }

}
