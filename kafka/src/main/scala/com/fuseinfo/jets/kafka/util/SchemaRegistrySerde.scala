package com.fuseinfo.jets.kafka.util

import com.fuseinfo.jets.kafka.ErrorHandler
import io.confluent.kafka.streams.serdes.avro.{GenericAvroDeserializer, GenericAvroSerializer}
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serdes, Serializer}

import javax.xml.bind.DatatypeConverter

class SchemaRegistrySerde(onErrors: List[ErrorHandler[GenericRecord]]) extends Serde[GenericRecord]{
  val inner: Serde[GenericRecord] =
    Serdes.serdeFrom(new GenericAvroSerializerWithLog(onErrors), new SchemaRegistryDeserializerWithLog(onErrors))

  override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = {
    inner.serializer().configure(configs, isKey)
    inner.deserializer().configure(configs, isKey)
  }

  override def close(): Unit = {
    inner.serializer().close()
    inner.deserializer().close()
  }

  override def serializer(): Serializer[GenericRecord] = inner.serializer

  override def deserializer(): Deserializer[GenericRecord] = inner.deserializer
}

class GenericAvroSerializerWithLog(onErrors: List[ErrorHandler[GenericRecord]]) extends GenericAvroSerializer {
  override def serialize(topic: String, record: GenericRecord): Array[Byte] = try {
    super.serialize(topic, record)
  } catch {
    case e:Throwable =>
      onErrors.foldLeft(null: GenericRecord){(res, errorProcessor) =>
        val output = errorProcessor(e, null, record)
        if (output != null) output else res
      }
      null
  }
}

class SchemaRegistryDeserializerWithLog(onErrors: List[ErrorHandler[GenericRecord]]) extends GenericAvroDeserializer {
  private val schema = SchemaBuilder.record("error").fields().requiredString("data").endRecord()

  override def deserialize(topic: String, bytes: Array[Byte]): GenericRecord = try {
    super.deserialize(topic, bytes)
  } catch {
    case e: Throwable =>
      val value = bytes match {
        case null => ""
        case _ => DatatypeConverter.printHexBinary(bytes)
      }
      val avroRecord = new GenericData.Record(schema)
      avroRecord.put("data", value)
      onErrors.foldLeft(null: GenericRecord){(res, errorHandler) =>
        val output = errorHandler(e, null, avroRecord)
        if (output != null) output else res
      }
  }
}