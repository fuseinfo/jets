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

import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode, ValueNode}
import com.fuseinfo.jets.kafka.{KafkaFlowBuilder, SchemaTransformerSupplier}
import com.fuseinfo.jets.kafka.store.ProcessorStore
import com.fuseinfo.jets.kafka.util.{AvroFunctionFactory, JsonUtils}
import com.fuseinfo.jets.util.AvroUtils
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.{ProcessorContext, PunctuationType, Punctuator, StateStore}
import org.apache.kafka.streams.state.{KeyValueStore, SessionStore, WindowStore}

import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.util.Try

class ScalaTransformer(paramNode: ObjectNode, keySchema: Schema, valueSchema: Schema)
  extends SchemaTransformerSupplier {

  private val outSchema = paramNode.get("schema") match {
    case valueNode:ValueNode => (new Schema.Parser).parse(valueNode.asText)
    case objNode: ObjectNode => (new Schema.Parser).parse(objNode.toString)
    case _ => null
  }
  private val valueMapping = paramNode.get("valueMapping").asInstanceOf[ObjectNode]
  private var transformer: SchemaTransformer = _
  private var storeDefMap: Map[String, String] = _

  override def get: Transformer[GenericRecord, GenericRecord, KeyValue[GenericRecord, GenericRecord]] = {
    transformer = new SchemaTransformer(keySchema, valueSchema, outSchema, valueMapping, paramNode, storeDefMap)
    transformer
  }

  override def setStoreMap(storeDefMap: Map[String, String]): Unit = {
    this.storeDefMap = storeDefMap
  }

  def recompileTransformer(objectNode: ObjectNode): Boolean = transformer.resetFunc(objectNode)

  override def reset(newNode: ObjectNode):Boolean = try {
    new ScalaTransformer(newNode, keySchema, valueSchema)
    transformer.resetFunc(newNode.get("valueMapping").asInstanceOf[ObjectNode])
  } catch {
    case _:Throwable => false
  }

  override def getEventCount: Long = if (transformer == null) 0 else transformer.counter

  override def getValueSchema: Schema = outSchema

  override def getProcessorSchema: String =
    """{"title": "ScalaTransformerSupplier","type": "object","properties": {
    "__function":{"type":"string","options":{"hidden":true}},
    "schema":{"type":"string","format":"json","description":"Avro Schema of the key field",
      "options":{"ace":{"useSoftTabs":true,"maxLines":16}}},
    "valueMapping":{"type":["object","string"],"description":"Mapping rule","properties": {
      "__init__":{"type":"string","format":"scala","options":{"ace":{"useSoftTabs":true,"maxLines":16}}},
      "__foreach__":{"type":"string","format":"scala","options":{"ace":{"useSoftTabs":true,"maxLines":16}}}},
      "additionalProperties":{"format":"scala","options":{"ace":{"useSoftTabs":true,"maxLines":16}}}},
    "stores":{"type":"string","description":"List of state stores, separated by comma"},
    "timers":{"type":["object","array"],"description":"Mapping rule","properties": {
      "__timer":{"type":"string","description":"timer class"},
      "interval":{"type":"integer","description":"Timer interval in ms"},
      "timeout":{"type":"integer","description":"Timer timeout in ms"}}},
    "processorStore":{"type":"string","description":"Processor Store for Timer"},
    "keyMapping":{"type":"object","description":"Create key by value","additionalProperties":{
      "format":"scala","options":{"ace":{"showLineNumbers":false,"useSoftTabs":true,"maxLines":16}}}},
    "timerMapping":{"type":"object","description":"key for storing pending messages","additionalProperties":{
      "format":"scala","options":{"ace":{"showLineNumbers":false,"useSoftTabs":true,"maxLines":16}}}},
    "timeout":{"type":"integer","description":"Async call timeout in ms"}
     },"required":["schema","valueMapping"]}"""

}

class SchemaTransformer(keySchema:Schema, valueSchema:Schema, outSchema:Schema, valueMapping:ObjectNode,
                        paramNode:ObjectNode, storeDefMap:Map[String, String])
  extends Transformer[GenericRecord, GenericRecord, KeyValue[GenericRecord, GenericRecord]] {

  private val timeout = JsonUtils.getOrElse(paramNode, "timeout", "15000").toLong
  private val storeNames = JsonUtils.get(paramNode,"stores").map(_.split(",").map(_.trim)).getOrElse(Array[String]())
  private var processorContext: ProcessorContext = _
  private val storeDefs = mutable.ArrayBuffer.empty[(StateStore, String, String)]
  @transient var counter = 0L

  private var ruleFunc: (GenericRecord, GenericRecord) => GenericRecord = _

  override def init(context: ProcessorContext): Unit = {
    processorContext = context
    storeDefs.clear
    storeNames.foreach{name =>
      val storeType = storeDefMap(name)
      val stateStore = context.getStateStore(name) match {
        case windowStore: WindowStore[_, _] if storeType contains "ProcessorStore" =>
          val defaultValue = AvroUtils.createEmpty(valueSchema)
          val emptyKey = AvroUtils.createEmpty(keySchema)
          val defaultKey = paramNode.get("timerMapping") match {
            case null => emptyKey
            case timerMapping:ObjectNode =>
              val schemaName = "_schema_" + keySchema.getName.replaceAll("[^a-zA-Z0-9_-]", "")
              val emptyFunc = AvroFunctionFactory.getFuncClass[(GenericRecord,GenericRecord) => GenericRecord](
                keySchema, valueSchema, keySchema, timerMapping, 0,
                s"($schemaName:org.apache.avro.Schema) extends ((org.apache.avro.generic.GenericRecord, " +
                  "org.apache.avro.generic.GenericRecord) => org.apache.avro.generic.GenericRecord)",
                "(_keyRecord:GenericRecord,_valueRecord:GenericRecord):GenericRecord",
                schemaName).getConstructor(classOf[Schema]).newInstance(keySchema)
              emptyFunc.apply(emptyKey, defaultValue)
            case _ => emptyKey
          }
          new ProcessorStore[GenericRecord,GenericRecord](
            windowStore.asInstanceOf[WindowStore[GenericRecord, GenericRecord]], defaultKey, defaultValue)
        case windowStore: WindowStore[_, _] => windowStore
        case sessionStore:SessionStore[_, _] => sessionStore
        case keyValueStore: KeyValueStore[_, _] => keyValueStore
        case _ => null
      }
      if (stateStore != null) {
        storeDefs += ((stateStore, name, storeType))
      }
    }

    ruleFunc = getFunc(keySchema, valueSchema, outSchema, valueMapping)

    paramNode.get("keyMapping") match {
      case keyMapping:ObjectNode =>
        val punctuateFunc = getPunctuateFunc(keySchema, valueSchema, outSchema, valueMapping)
        val keyFunc = getFunc(keySchema, valueSchema, keySchema, keyMapping)
        paramNode.get("timers") match {
          case array:ArrayNode =>
            array.elements().foreach(node => setTimer(context, punctuateFunc, keyFunc, node.asInstanceOf[ObjectNode]))
          case objNode:ObjectNode => setTimer(context, punctuateFunc, keyFunc, objNode)
          case _ =>
        }
      case _ =>
    }
  }

  private def setTimer(context: ProcessorContext, punctuateFunc: (GenericRecord,GenericRecord,Boolean)=>GenericRecord,
                       keyFunc: (GenericRecord, GenericRecord) => GenericRecord, childNode:ObjectNode) = {
    val interval = JsonUtils.getOrElse(childNode, "interval", "100").toLong
    val className = childNode.get("__timer").asText
    val clazz = Try(Class.forName(className))
      .getOrElse(Class.forName(KafkaFlowBuilder.packagePrefix + "timer." + className))
    val (_, processStore) = getProcessStore.get
    val timer = clazz.getDeclaredConstructor(classOf[ProcessorContext],
      classOf[(GenericRecord,GenericRecord,Int)=>GenericRecord],
      classOf[(GenericRecord, GenericRecord) => GenericRecord], classOf[ObjectNode],
      classOf[ProcessorStore[GenericRecord, GenericRecord]])
      .newInstance(context, punctuateFunc, keyFunc, childNode, processStore).asInstanceOf[Punctuator]
    context.schedule(interval, PunctuationType.WALL_CLOCK_TIME, timer)
  }

  override def transform(key: GenericRecord, value: GenericRecord): KeyValue[GenericRecord, GenericRecord] = {
    val newVal = ruleFunc(key, value)
    if (newVal != null) {
      counter += 1
      new KeyValue(key, newVal)
    } else null
  }

  override def close(): Unit = {}

  def resetFunc(mapping:ObjectNode): Boolean =
    scala.util.Try(ruleFunc = getFunc(keySchema, valueSchema, outSchema, mapping)).isSuccess

  private def getFunc(keySchema:Schema, valueSchema:Schema, outSchema:Schema, valueMapping:ObjectNode):
      (GenericRecord, GenericRecord) => GenericRecord = {
    val init = storeDefs.zipWithIndex
      .map(t => s"val ${t._1._2}=_context(${t._2}).asInstanceOf[${t._1._3}]\n").mkString("")
    val stateStores = storeDefs.map(_._1).toArray

    if (stateStores.nonEmpty) {
      val foreach = getProcessStore match {
        case Some((storeName, _)) =>
          s"""  def findEvent(_func: GenericRecord => Boolean, _ts:Long=System.currentTimeMillis) = $storeName.fetchLast(_keyRecord, _func, 0L, _ts)
  def saveEvent(_ts:Long=System.currentTimeMillis) = {
    $storeName.putUnique(_keyRecord, _valueRecord, _ts)
    null
  }
  def toRetry(_ts:Long=System.currentTimeMillis) = $storeName.putUnique($storeName.defaultKey, _valueRecord, _ts)
  def loadEvent(_cond:Map[String, AnyRef], _ts:Long=System.currentTimeMillis) = {
    val _loadKey = if (_cond.nonEmpty) {
      val _newKey = new GenericData.Record(_keyRecord.asInstanceOf[GenericData.Record], true)
      _cond.foreach(_p => _newKey.put(_p._1, _p._2))
      _newKey
    } else _keyRecord
    $storeName.fetchLast(_loadKey, 0L, _ts)
  }
"""
        case None => ""
      }
      val schemaName = "_schema_" + outSchema.getName.replaceAll("[^a-zA-Z0-9_-]", "")
      AvroFunctionFactory.getFuncClass[(GenericRecord,GenericRecord) => GenericRecord](keySchema,
        valueSchema, outSchema, valueMapping, timeout,
     s"($schemaName:org.apache.avro.Schema, _context:Array[AnyRef]) extends ((org.apache.avro.generic.GenericRecord," +
          " org.apache.avro.generic.GenericRecord) => org.apache.avro.generic.GenericRecord)",
        "(_keyRecord:GenericRecord,_valueRecord:GenericRecord):GenericRecord",schemaName, init, foreach)
        .getDeclaredConstructor(classOf[Schema], classOf[Array[AnyRef]])
        .newInstance(outSchema, stateStores)
    } else {
      val schemaName = "_schema_" + outSchema.getName.replaceAll("[^a-zA-Z0-9_-]", "")
      AvroFunctionFactory.getFuncClass[(GenericRecord,GenericRecord) => GenericRecord](keySchema,
      valueSchema, outSchema, valueMapping, timeout,
        s"($schemaName:org.apache.avro.Schema) extends ((org.apache.avro.generic.GenericRecord, " +
          "org.apache.avro.generic.GenericRecord) => org.apache.avro.generic.GenericRecord)",
      "(_keyRecord:GenericRecord,_valueRecord:GenericRecord):GenericRecord",
        schemaName).getConstructor(classOf[Schema]).newInstance(outSchema)}
  }

  private def getPunctuateFunc(keySchema:Schema, valueSchema:Schema, outSchema:Schema, valueMapping:ObjectNode) = {
    val init = storeDefs.zipWithIndex
      .map(t => s"val ${t._1._2}=_context(${t._2}).asInstanceOf[${t._1._3}]\n").mkString("")
    val stateStores = storeDefs.map(_._1).toArray
    if (stateStores.nonEmpty) {
      val storeName = getProcessStore.get._1
      val foreach = s"""  def findEvent(_func: GenericRecord => Boolean, _ts:Long=System.currentTimeMillis) = {
    val _rec = $storeName.fetchLast(_keyRecord, _func, 0L, _ts)
    if (_rec != null) _rec else if (_isExpired) $storeName.defaultValue else null
  }
  def saveEvent(_ts:Long=System.currentTimeMillis) = {
    $storeName.put(_keyRecord, _valueRecord, _ts)
    null
  }
  def toRetry(_ts:Long = System.currentTimeMillis) = {}
  def loadEvent(_cond:Map[String, AnyRef], _ts:Long=System.currentTimeMillis) = {
    val _loadKey = if (_cond.nonEmpty) {
      val _newKey = new GenericData.Record(_keyRecord.asInstanceOf[GenericData.Record], true)
      _cond.foreach(_p => _newKey.put(_p._1, _p._2))
      _newKey
    } else _keyRecord
    val _rec = $storeName.fetchLast(_loadKey, 0L, _ts)
    if (_rec != null) _rec else if (_isExpired) $storeName.defaultValue else null
  }
"""
      val schemaName = "_schema_" + outSchema.getName.replaceAll("[^a-zA-Z0-9_-]", "")
      AvroFunctionFactory.getFuncClass[(GenericRecord,GenericRecord,Boolean) => GenericRecord](keySchema,
        valueSchema, outSchema, valueMapping, timeout,
      s"($schemaName:org.apache.avro.Schema, _context:Array[AnyRef]) extends ((org.apache.avro.generic.GenericRecord" +
          ",org.apache.avro.generic.GenericRecord,Boolean) => org.apache.avro.generic.GenericRecord)",
        "(_keyRecord:GenericRecord,_valueRecord:GenericRecord,_isExpired:Boolean):GenericRecord",
        schemaName, init, foreach).getDeclaredConstructor(classOf[Schema], classOf[Array[AnyRef]])
        .newInstance(outSchema, stateStores)
    } else null
  }

  private def getProcessStore:Option[(String, ProcessorStore[GenericRecord, GenericRecord])]= try {
    val storeName = paramNode.get("processorStore") match {
      case null => paramNode.get("stores").asText.split(",")(0).trim
      case json => json.asText
    }
    val storeMap = storeDefs.map(t => t._2 -> t._1).toMap
    Some((storeName, storeMap(storeName).asInstanceOf[ProcessorStore[GenericRecord, GenericRecord]]))
  } catch {
    case _:Exception => None
  }
}
