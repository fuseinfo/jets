package com.fuseinfo.jets.kafka.function

import com.fasterxml.jackson.databind.node.ObjectNode
import com.fuseinfo.jets.kafka.util.{AvroFunctionFactory, JsonUtils}
import com.fuseinfo.jets.kafka.{AvroPredicate, ErrorHandler, ErrorLogger, KafkaFlowBuilder}
import com.fuseinfo.jets.util.VarUtils
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.slf4j.LoggerFactory

class ScalaPredicate(stepName: String, paramNode: ObjectNode, keySchema: Schema, valueSchema: Schema)
  extends AvroPredicate {

  private val logger = LoggerFactory.getLogger(this.getClass)

  private val testString = VarUtils.enrichString(paramNode.get("test").asText, KafkaFlowBuilder.vars)
  private var testFunc = getTest(keySchema, valueSchema, testString)
  @transient private var counter = 0L

  private val onErrors = JsonUtils.initErrorFuncs(stepName, paramNode.get("onError")) match {
    case Nil => new ErrorLogger(stepName, logger) :: Nil
    case list => list
  }

  private def getTest(keySchema:Schema, valueSchema:Schema, test:String)= {
    AvroFunctionFactory.getTestClass(keySchema, valueSchema, test).newInstance()
  }

  override def test(key: GenericRecord, value: GenericRecord): Boolean = {
    val result = try {
      testFunc(key, value)
    } catch {
      case e: Exception =>
        onErrors.foreach(errorProcessor => errorProcessor(e, key, value))
        false
    }
    if (result) counter += 1
    result
  }

  override def reset(newNode: ObjectNode):Boolean =
    scala.util.Try(testFunc = new ScalaPredicate(stepName, newNode, keySchema, valueSchema).testFunc).isSuccess

  override def getEventCount: Long = counter

  override def getProcessorSchema: String =
    """{"title": "ScalaPredicate","type": "object","properties": {
    "__function":{"type":"string","options":{"hidden":true}},
    "test":{"type":"string","format":"scala","description":"test function to select records",
      "options":{"ace":{"showLineNumbers":false,"useSoftTabs":true,"maxLines":32}}}
    },"required":["test"]}"""
}
