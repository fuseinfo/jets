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

import com.fuseinfo.jets.beam.store.{ProcessorStore, StateStore}
import com.fuseinfo.jets.beam.{BeamFlowBuilder, TransformerFn}
import com.fuseinfo.jets.beam.util.AvroFunctionFactory
import com.fuseinfo.jets.util.AvroUtils
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.state.{TimeDomain, Timer, TimerSpecs}
import org.apache.beam.sdk.transforms.DoFn.{Element, OnTimer, OutputReceiver, ProcessElement, TimerId}
import org.apache.beam.sdk.values.KV
import org.joda.time.Duration

import scala.util.Try

class ScalaTransformer(paramNode:java.util.Map[String, AnyRef], keySchema:String, valueSchema:String)
  extends TransformerFn(keySchema) {

  @transient private lazy val parser = new Schema.Parser()
  @transient private lazy val keyOutSchema = if (keySchema != null) parser.parse(keySchema) else null
  @transient private lazy val valueInSchema = parser.parse(valueSchema)
  @transient private lazy val valueOutSchema = paramNode.get("schema") match {
    case null => null
    case valueNode => (new Schema.Parser).parse(valueNode.toString)
  }

  private def emptyKey = AvroUtils.createEmpty(keyOutSchema)
  private def defaultValue = AvroUtils.createEmpty(valueInSchema)
  @transient private lazy val defaultKey = paramNode.get("timerMapping") match {
    case null => emptyKey
    case timerMapping:java.util.Map[_, _] =>
      val schemaName = "_schema_" + keyOutSchema.getName.replaceAll("[^a-zA-Z0-9_-]", "")
      val emptyFunc = AvroFunctionFactory.getFuncClass[(GenericRecord,GenericRecord) => GenericRecord](
        keyOutSchema, valueInSchema, keyOutSchema, timerMapping.asInstanceOf[java.util.Map[String, AnyRef]], 0,
        s"($schemaName:org.apache.avro.Schema) extends ((org.apache.avro.generic.GenericRecord, " +
          "org.apache.avro.generic.GenericRecord) => org.apache.avro.generic.GenericRecord)",
        "(_keyRecord:GenericRecord,_valueRecord:GenericRecord):GenericRecord",
        schemaName).getConstructor(classOf[Schema]).newInstance(keyOutSchema)
      emptyFunc.apply(emptyKey, defaultValue)
    case _ => emptyKey
  }

  private val interval = {
    val timeNode = paramNode.getOrDefault("timers", new java.util.HashMap[String, AnyRef]) match {
      case array:Array[_] if array.length > 0 => array(0)
      case mapNode:java.util.Map[_, _] => mapNode
      case _ => new java.util.HashMap[String, AnyRef]
    }
    val intervalSize = timeNode match {
      case timeNode: java.util.Map[_, _] =>
        timeNode.asInstanceOf[java.util.Map[String, AnyRef]].getOrDefault("interval", "100").toString.toLong
      case _ => 100L
    }
    Duration.millis(intervalSize)
  }

  private lazy val valueFunc: (GenericRecord, GenericRecord) => GenericRecord = getFunc(keyOutSchema, valueInSchema,
    valueOutSchema, paramNode.get("valueMapping").asInstanceOf[java.util.Map[String, AnyRef]])

  private lazy val timer: Option[((GenericRecord, GenericRecord) => Unit) => Unit] =
    paramNode.get("timers") match {
      case null => None
      case array: Array[_] if array.length > 0 => createTimer(array(0).asInstanceOf[java.util.Map[String, AnyRef]])
      case timeNode: java.util.Map[_, _] => createTimer(timeNode.asInstanceOf[java.util.Map[String, AnyRef]])
      case _ => None
    }

  private def createTimer(timeNode:java.util.Map[String, AnyRef]) = {
    timeNode.get("__timer") match{
      case null => None
      case name =>
      val className = name.toString
      val keyFunc = getFunc(keyOutSchema, valueInSchema, keyOutSchema,
        paramNode.get("keyMapping").asInstanceOf[java.util.Map[String, AnyRef]])
      val timerFunc = getTimerFunc(keyOutSchema, valueInSchema, valueOutSchema,
        paramNode.get("valueMapping").asInstanceOf[java.util.Map[String, AnyRef]])
      val clazz = Try(Class.forName(className)).getOrElse(Class.forName(BeamFlowBuilder.packagePrefix + "timer." + className))
      Some(clazz.getConstructor(classOf[(GenericRecord, GenericRecord, Boolean) => GenericRecord],
        classOf[(GenericRecord, GenericRecord) => GenericRecord], classOf[java.util.Map[String, AnyRef]],
        classOf[ProcessorStore], classOf[GenericRecord], classOf[java.lang.Long])
        .newInstance(timerFunc, keyFunc, timeNode,
          stateStores.find(_._2.isInstanceOf[ProcessorStore]).get._2.asInstanceOf[ProcessorStore], defaultKey,
          new java.lang.Long(timeNode.getOrDefault("interval", "100").toString))
        .asInstanceOf[((GenericRecord, GenericRecord) => Unit) => Unit])
    }
  }

  private var storeMap:java.util.Map[String, java.util.Map[String, AnyRef]] =
    new java.util.HashMap[String, java.util.Map[String, AnyRef]]
  override def setStoreMap(storeMap: java.util.Map[String, java.util.Map[String, AnyRef]]): Unit = {
    this.storeMap = storeMap
  }

  lazy val stateStores: Array[(String, StateStore)] = paramNode.get("stores") match {
    case null => Array.empty[(String, StateStore)]
    case storeList:String => storeList.split(",")
      .map(storeName => storeName -> storeMap.get(storeName)).filter(_._2 != null)
      .map{case (storeName, props) =>
        val storeClass = String.valueOf(props.get("__store"))
        val clazz = try {
          Class.forName(storeClass)
        } catch {
          case _:ClassNotFoundException => Class.forName(BeamFlowBuilder.packagePrefix + "store." + storeClass)
        }
        (storeName, clazz.getConstructor(classOf[java.util.Map[String, AnyRef]], classOf[Schema], classOf[Schema])
          .newInstance(props, keyOutSchema, valueInSchema).asInstanceOf[StateStore])
      }
    case _ => Array.empty[(String, StateStore)]
  }

  private def getFunc(keySchema:Schema, valueSchema:Schema, outSchema:Schema, mapping:java.util.Map[String,AnyRef])= {
    val schemaName = "_schema_" + outSchema.getName.replaceAll("[^a-zA-Z0-9_-]", "")
    if (stateStores.nonEmpty) {
      val init = stateStores.zipWithIndex
        .map{t =>
          val instanceType = t._1._2 match {
            case _:ProcessorStore => "store.ProcessorStore"
            case _ => "store.StateStore"
          }
          s"val ${t._1._1}=_context(${t._2}).asInstanceOf[$instanceType]"
        }.mkString("\n")
      val foreach = stateStores.find(_._2.isInstanceOf[ProcessorStore]) match {
        case Some((storeName, _:ProcessorStore)) =>
          s"""  def saveEvent(_ts:Long=System.currentTimeMillis) = {
    $storeName.put(_keyRecord, _valueRecord, _ts)
    null
  }
  def toRetry(_ts:Long=System.currentTimeMillis) = {
    $storeName.putRetry(_retryKey, _valueRecord, _ts)
    null
  }
  def loadEvent(_cond:Map[String, AnyRef], _ts:Long=System.currentTimeMillis) = {
    val _loadKey = if (_cond.nonEmpty) {
      val _newKey = new GenericData.Record(_keyRecord.asInstanceOf[GenericData.Record], true)
      _cond.foreach(_p => _newKey.put(_p._1, _p._2))
      _newKey
    } else _keyRecord
    $storeName.fetchLast(_loadKey, 0L, _ts)
  }
"""
        case _ => ""
      }
      AvroFunctionFactory.getFuncClass[(GenericRecord, GenericRecord) => GenericRecord](keySchema, valueSchema, outSchema, mapping, 0,
        s"($schemaName:org.apache.avro.Schema, _context:Array[AnyRef], _retryKey:org.apache.avro.generic.GenericRecord) " +
          "extends ((org.apache.avro.generic.GenericRecord,org.apache.avro.generic.GenericRecord) => org.apache.avro.generic.GenericRecord)",
        "(_keyRecord:GenericRecord,_valueRecord:GenericRecord):GenericRecord", schemaName, init, foreach)
        .getConstructor(classOf[Schema], classOf[Array[AnyRef]], classOf[GenericRecord])
        .newInstance(outSchema, stateStores.map(_._2), defaultKey)

    } else {
      AvroFunctionFactory.getFuncClass[(GenericRecord, GenericRecord) => GenericRecord](keySchema, valueSchema, outSchema, mapping, 0,
        s"($schemaName:org.apache.avro.Schema) " +
          "extends ((org.apache.avro.generic.GenericRecord,org.apache.avro.generic.GenericRecord) => org.apache.avro.generic.GenericRecord)",
        "(_keyRecord:GenericRecord,_valueRecord:GenericRecord):GenericRecord", schemaName)
        .getConstructor(classOf[Schema]).newInstance(outSchema)
    }
  }

  private def getTimerFunc(keySchema:Schema, valueSchema:Schema, outSchema:Schema, mapping:java.util.Map[String,AnyRef])= {
    val init = stateStores.zipWithIndex
      .map{t =>
        val instanceType = t._1._2 match {
          case _:ProcessorStore => "store.ProcessorStore"
          case _ => "store.StateStore"
        }
        s"val ${t._1._1}=_context(${t._2}).asInstanceOf[$instanceType]"
      }.mkString("\n")
    val storeName = stateStores.find(_._2.isInstanceOf[ProcessorStore]).get._1
    val foreach = s"""  def saveEvent(_ts:Long=System.currentTimeMillis) = {
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
    AvroFunctionFactory.getFuncClass[(GenericRecord, GenericRecord, Boolean) => GenericRecord](keySchema, valueSchema, outSchema, mapping, 0,
      s"($schemaName:org.apache.avro.Schema, _context:Array[AnyRef]) " +
   "extends ((org.apache.avro.generic.GenericRecord,org.apache.avro.generic.GenericRecord,Boolean)=>org.apache.avro.generic.GenericRecord)",
      "(_keyRecord:GenericRecord,_valueRecord:GenericRecord,_isExpired:Boolean):GenericRecord", schemaName, init, foreach)
      .getConstructor(classOf[Schema], classOf[Array[AnyRef]]).newInstance(outSchema, stateStores.map(_._2))
  }

  var timerSet:Boolean = false

  @TimerId("punctuator")
  private val punctuatorSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME)

  @OnTimer("punctuator")
  def onPunctuator(context:OnTimerContext, @TimerId("punctuator") punctuator:Timer): Unit = {
    val toOutput = (k:GenericRecord, v:GenericRecord) => context.output(KV.of(k, v))

    timer.foreach{func =>
      func(toOutput)
      punctuator.offset(interval).setRelative()
    }
  }

  @ProcessElement
  def process(@Element input:KV[GenericRecord, GenericRecord], @TimerId("punctuator") punctuator:Timer,
              receiver:OutputReceiver[KV[GenericRecord, GenericRecord]]):Unit = {
    if (!timerSet) {
      timer match {
        case Some(_) => punctuator.offset(interval).setRelative()
        case None =>
      }
      timerSet = true
    }
    val outValue = valueFunc(input.getKey, input.getValue)
    if (outValue != null) receiver.output(KV.of(input.getKey, outValue))
  }

  override def getValueSchema: Schema = valueOutSchema

  override def getKeySchema: Schema = keyOutSchema
}
