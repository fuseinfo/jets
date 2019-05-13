package com.fuseinfo.jets.kafka.function

import com.fasterxml.jackson.databind.node.ObjectNode
import com.fuseinfo.jets.kafka.util.AvroFunctionFactory
import com.fuseinfo.jets.kafka.{AvroPredicate, KafkaFlowBuilder}
import com.fuseinfo.jets.util.VarUtils
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

class ScalaPredicate(paramNode: ObjectNode, keySchema: Schema, valueSchema: Schema) extends AvroPredicate {
  private val testString = VarUtils.enrichString(paramNode.get("test").asText, KafkaFlowBuilder.vars)
  private var testFunc = getTest(keySchema, valueSchema, testString)
  @transient private var counter = 0L

  private def getTest(keySchema:Schema, valueSchema:Schema, test:String)= {
    AvroFunctionFactory.getTestClass(keySchema, valueSchema, test).newInstance()
  }

  override def test(key: GenericRecord, value: GenericRecord): Boolean = {
    val result = testFunc(key, value)
    if (result) counter += 1
    result
  }

  override def reset(newNode: ObjectNode):Boolean =
    scala.util.Try(testFunc = new ScalaPredicate(newNode, keySchema, valueSchema).testFunc).isSuccess

  override def getEventCount: Long = counter

  override def getProcessorSchema: String =
    """{"title": "ScalaPredicate","type": "object","properties": {
    "__function":{"type":"string","options":{"hidden":true}},
    "test":{"type":"string","format":"scala","description":"test function to select records",
      "options":{"ace":{"showLineNumbers":false,"useSoftTabs":true,"maxLines":32}}}
    },"required":["test"]}"""
}
