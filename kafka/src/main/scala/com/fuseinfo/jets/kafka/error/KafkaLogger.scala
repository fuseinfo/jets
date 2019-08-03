package com.fuseinfo.jets.kafka.error

import java.util.Properties

import com.fasterxml.jackson.databind.node.ObjectNode
import com.fuseinfo.jets.kafka.ErrorHandler
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.collection.JavaConversions._

class KafkaLogger(stepName:String, paramNode:ObjectNode) extends ErrorHandler{
  private val topic = paramNode.get("topic").asText
  private val producer = {
    val props = new Properties
    paramNode.fields.foreach{entry =>
      val prop = entry.getKey
      if (!prop.startsWith("_") && prop != "topic") props.put(prop, entry.getValue.asText)
    }
    new KafkaProducer[String, String](props)
  }

  override def apply(e: Exception, key: GenericRecord, value: GenericRecord): GenericRecord = {
    producer.send(new ProducerRecord[String, String](topic, stepName, String.valueOf(value)))
    null
  }
}
