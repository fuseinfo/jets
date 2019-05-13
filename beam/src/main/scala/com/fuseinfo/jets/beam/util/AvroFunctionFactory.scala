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

package com.fuseinfo.jets.beam.util

import com.fuseinfo.common.ScalaCompiler
import org.apache.avro.{LogicalTypes, Schema}
import org.apache.avro.Schema.Type
import org.apache.avro.generic.GenericRecord

import scala.collection.JavaConversions._

object AvroFunctionFactory {
  private val IMPORTS = Array("com.fuseinfo.jets.util._","com.fuseinfo.jets.beam.util._","org.apache.beam.sdk.values._",
    "org.apache.avro.generic._", "scala.concurrent._", "ExecutionContext.Implicits.global","com.fuseinfo.jets.beam._")
    .map("import " + _ + "\n").mkString

  private val FUTURE_ADD1 = "    futureList += Future{"
  private val FUTURE_ADD2 = "}\n"
  private val FUTURE_DEF = "val futureList = scala.collection.mutable.Buffer.empty[Future[_]]\n"
  private def futureWait(timeout:Long = 15000L) = s"""val timeout = System.currentTimeMillis + $timeout
scala.util.Try(futureList.foreach{f =>
  Await.ready(f, new duration.FiniteDuration(timeout - System.currentTimeMillis,duration.MILLISECONDS))})
"""

  def getFuncClass[T](keySchema:Schema, valueSchema:Schema, outSchema:Schema, mapping:java.util.Map[String, AnyRef], timeout:Long,
                      funcType:String, applyDef:String, schemaName:String, init:String = "", foreach:String = ""): Class[T] = {
    try {
      val src = scalaRecordGen(keySchema, valueSchema, outSchema, mapping, schemaName, timeout,true, funcType, applyDef, IMPORTS + init, foreach)
      new ScalaCompiler().compile[T](src)
    } catch {
      case e:Throwable => throw e
    }
  }

  def getTestClass(keySchema:Schema, valueSchema:Schema, test:String):Class[(GenericRecord, GenericRecord) => Boolean] = {
    val src = scalaBooleanGen(keySchema, valueSchema, test)
    try {
      new ScalaCompiler().compile[(GenericRecord, GenericRecord) => Boolean](src)
    } catch {
      case e:Throwable => throw e
    }
  }

  private def getFieldSchema(field:Schema.Field) = {
    val schema = field.schema
    schema.getType match {
      case Type.UNION => field.schema.getTypes.find(s => s.getType != Type.NULL) match {
        case Some(fSchema) => Some(fSchema)
        case None => None
      }
      case _ => Some(schema)
    }
  }

  private def generateVars(inSchema: Schema, varName:String, prefix:String):String = {
    inSchema.getFields.zipWithIndex.map{case (field, i) =>
      val fieldName = prefix + field.name
      s"    lazy val $fieldName = $varName.get($i)" + {
        val (fType, lType) = getFieldSchema(field).map(f=> (f.getType, f.getLogicalType)).getOrElse((Type.NULL, null))
        (fType, lType) match {
          case (Type.RECORD, _) =>
            ".asInstanceOf[GenericRecord]\n" + generateVars(field.schema, fieldName, fieldName + "$")
          case (Type.ARRAY, _) => ".asInstanceOf[java.util.Collection[AnyRef]]"
          case (Type.BOOLEAN, _) => ".asInstanceOf[java.lang.Boolean]"
          case (Type.BYTES, null) => ".asInstanceOf[java.nio.ByteBuffer]"
          case (Type.BYTES, logicalType:LogicalTypes.Decimal) => ".asDecimal(" + logicalType.getScale + ")"
          case (Type.DOUBLE, _) => ".asInstanceOf[java.lang.Double]"
          case (Type.FIXED, null) => ".asInstanceOf[GenericFixed]"
          case (Type.FIXED, logicalType:LogicalTypes.Decimal) => ".asDecimal(" + logicalType.getScale + ")"
          case (Type.FLOAT, _) => ".asInstanceOf[java.lang.Float]"
          case (Type.INT, null) => ".asInstanceOf[java.lang.Integer]"
          case (Type.INT, logicalType:LogicalTypes.Date) => ".asDate()"
          case (Type.LONG, null) => ".asInstanceOf[java.lang.Long]"
          case (Type.LONG, logicalType:LogicalTypes.TimestampMillis) => ".asTimestampMillis()"
          case (Type.LONG, logicalType:LogicalTypes.TimestampMicros) => ".asTimestampMicros()"
          case (Type.MAP, _) =>
            val mSchema = getFieldSchema(field).get.getValueType
            val mReal = mSchema.getType match {
              case Type.UNION => field.schema.getTypes.find(s => s.getType != Type.NULL) match {
                case Some(fSchema) => fSchema
                case None => mSchema
              }
              case _ => mSchema
            }
            val valueType = mReal.getType match {
              case Type.BOOLEAN => "java.lang.Boolean"
              case Type.BYTES => "java.nio.ByteBuffer"
              case Type.DOUBLE => "java.lang.Double"
              case Type.FIXED => "GenericFixed"
              case Type.FLOAT => "java.lang.Float"
              case Type.INT => "java.lang.Integer"
              case Type.LONG => "java.lang.Long"
              case Type.STRING => "String"
              case _ => "AnyRef"
            }
            s".asStringMap[$valueType]"
          case (Type.STRING, _) => ".asString"
          case _ => ""
        }
      } + "\n"
    }.mkString
  }

  private def scalaRecordGen(keySchema:Schema, valueSchema:Schema, outSchema:Schema, mapping:java.util.Map[String, AnyRef], schemaName:String, timeout:Long,
                             isRoot:Boolean = false, funcType:String = "new ((GenericRecord, GenericRecord) => GenericRecord)",
                             applyDef:String = "(_keyRecord:GenericRecord,_valueRecord:GenericRecord):GenericRecord",
                             init:String = "", foreach:String = ""): String = {
    val outIndexMap = outSchema.getFields.zipWithIndex.map(p => p._1.name -> p._2).toMap
    val varSb = if (isRoot) new StringBuilder(foreach + generateVars(valueSchema, "_valueRecord", ""))
    else new StringBuilder
    var hasFuture = false
    val mapDef = mapping.filterNot(_._1.startsWith("__")).map{entry =>
      val entryKey = entry._1.trim
      val (fName, bg) =
        if (entryKey.endsWith("&")) (entryKey.substring(0, entryKey.length - 1).trim, true) else (entryKey, false)
      hasFuture |= bg
      val (fa1, fa2) = if (bg) (FUTURE_ADD1, FUTURE_ADD2) else ("", "\n")
      val outIndex = outIndexMap(fName)
      val childSchema = getFieldSchema(outSchema.getField(fName)).get
      val setStat = entry._2 match {
        case childObj:java.util.Map[_, _] =>
          val childSchemaName = "_schema_" + childSchema.getName.replaceAll("[^a-zA-Z0-9_-]", "")
          varSb.append(s"  val $childSchemaName = _fieldArr($outIndex).schema\n")
          varSb.append(s"  val ${fName}_func = " + scalaRecordGen(keySchema, valueSchema, childSchema,
            childObj.asInstanceOf[java.util.Map[String, AnyRef]], childSchemaName, timeout))
          s"_recBuilder.set(_fieldArr($outIndex), ${fName}_func(_keyRecord,_valueRecord))"
        case textNode:String =>
          val text = textNode match {
            case "" => "\"\""
            case nonEmpty => nonEmpty
          }
          (childSchema.getType, childSchema.getLogicalType) match {
            case (Type.ARRAY, _) =>
              s"_recBuilder.setArray(_fieldArr($outIndex), {$text})"
            case (Type.BOOLEAN, _) =>
              s"_recBuilder.setBoolean(_fieldArr($outIndex), {$text})"
            case (Type.BYTES, null) =>
              s"_recBuilder.setBytes(_fieldArr($outIndex), {$text})"
            case (Type.DOUBLE, _) =>
              s"_recBuilder.setDouble(_fieldArr($outIndex), {$text})"
            case (Type.FIXED, null) =>
              s"_recBuilder.set(_fieldArr($outIndex), {$text})"
            case (Type.FLOAT, _) =>
              s"_recBuilder.setFloat(_fieldArr($outIndex), {$text})"
            case (Type.INT, null) =>
              s"_recBuilder.setInt(_fieldArr($outIndex), {$text})"
            case (Type.LONG, null) =>
              s"_recBuilder.setLong(_fieldArr($outIndex), {$text})"
            case (Type.MAP, _) =>
              s"_recBuilder.setMap(_fieldArr($outIndex), {$text})"
            case (Type.STRING, _) =>
              s"_recBuilder.setString(_fieldArr($outIndex), {$text})"
            case (Type.BYTES, logicType:LogicalTypes.Decimal) =>
              val scale = logicType.getScale
              s"_recBuilder.setDecimalBytes(_fieldArr($outIndex), {$text}, $scale)"
            case (Type.FIXED, logicType:LogicalTypes.Decimal) =>
              val scale = logicType.getScale
              s"_recBuilder.setDecimalFixed(_fieldArr($outIndex), {$text}, $scale)"
            case (Type.LONG, logicType:LogicalTypes.TimestampMillis) =>
              s"_recBuilder.setTimestampMillis(_fieldArr($outIndex), {$text})"
            case (Type.LONG, logicType:LogicalTypes.TimestampMicros) =>
              s"_recBuilder.setTimestampMicros(_fieldArr($outIndex), {$text})"
            case (Type.INT, logicalType: LogicalTypes.Date) =>
              s"_recBuilder.setDate(_fieldArr($outIndex), {$text})"
            case _ =>
          }
        case _ =>
      }
      if (bg) {FUTURE_ADD1 + setStat + FUTURE_ADD2} else {"    " + setStat + "\n"}
    }.toSeq.mkString
    val initDef = mapping.get("__init__") match {
      case null => ""
      case initNode => initNode.toString
    }
    val foreachDef = mapping.get("__foreach__") match {
      case null => ""
      case foreachNode => foreachNode.toString
    }
    val beforeBuild = if (hasFuture) {
      varSb.append(FUTURE_DEF)
      futureWait(timeout)
    } else ""
    val varDef = varSb.toString
    s"""$funcType {
  $init
  val _recBuilder = new AvroBuilder($schemaName)
  val _fields = $schemaName.getFields
  val _fieldArr = _fields.toArray(new Array[org.apache.avro.Schema.Field](_fields.size))
$initDef
  override def apply$applyDef = {
$varDef
$foreachDef
$mapDef
$beforeBuild
    _recBuilder.build()
  }
}"""
  }

  private def scalaBooleanGen(keySchema:Schema, valueSchema:Schema, test:String): String = {
    val varDef = generateVars(valueSchema, "_valueRecord", "")
    s""" extends ((org.apache.avro.generic.GenericRecord,org.apache.avro.generic.GenericRecord) => Boolean) {
  $IMPORTS
  override def apply(_keyRecord:GenericRecord,_valueRecord:GenericRecord):Boolean = {
$varDef
$test
  }
}"""
  }
}
