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
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.schemas.Schema.FieldType
import org.apache.beam.sdk.values.{KV, Row}

import scala.collection.JavaConversions._

object RowFunctionFactory {
  private val IMPORTS = "import org.apache.beam.sdk.schemas.Schema\nimport " + getClass.getPackage.getName +
    "._\nimport org.apache.beam.sdk.values._\nimport scala.concurrent._\nimport ExecutionContext.Implicits.global\n"

  def getFuncClass[T](keySchema:Schema, valueSchema:Schema, outSchema:Schema, mapping:java.util.Map[String, AnyRef], timeout:Long,
                      funcType:String, applyDef:String, schemaName:String, init:String = "", foreach:String = ""): Class[T] = {
    try {
      val src = scalaRowGen(keySchema, valueSchema, outSchema, mapping, schemaName, timeout,true, funcType, applyDef, IMPORTS + init, foreach)
      new ScalaCompiler().compile[T](src)
    } catch {
      case e:Throwable => throw e
    }
  }

  private def scalaRowGen(keySchema: Schema, valueSchema: Schema, outSchema: Schema, mapping: java.util.Map[String, AnyRef], schemaName: String,
                          timeout: Long, isRoot: Boolean = false, funcType: String = "new ((Row, Row) => Row)",
                          applyDef: String = "(_keyRecord:Row,_valueRecord:Row):Row",
                          init: String = "", foreach: String = ""): String = {
    val varSb = if (isRoot) new StringBuilder(foreach + generateVars(valueSchema, "_valueRecord", ""))
    else new StringBuilder
    val mapDef = outSchema.getFields.zipWithIndex.map{case (field, i) =>
      val fieldName = field.getName
      val fieldDef = mapping.get(fieldName)
      "_recBuilder." + (
        fieldDef match {
          case null => s"addValue(null)"
          case ref =>
            ref match {
              case childMap: java.util.Map[_, _] =>
                val childSchema = field.getType.getRowSchema
                val childSchemaName = "_schema_" + fieldName
                varSb.append(s"  val $childSchemaName = $schemaName.getField($i).getType.getRowSchema\n")
                varSb.append(s"  val ${fieldName}_func = " + scalaRowGen(keySchema, valueSchema, childSchema,
                  childMap.asInstanceOf[java.util.Map[String, AnyRef]], childSchemaName, timeout))
                s"addValue(${fieldName}_func(_keyRecord,_valueRecord))"
              case text: String =>
                field.getType match {
                  case FieldType.BOOLEAN => s"addBoolean{$text}"
                  case FieldType.BYTE => s"addByte{$text}"
                  case FieldType.BYTES => s"addBytes{$text}"
                  case FieldType.DATETIME => s"addDateTime{$text}"
                  case FieldType.DECIMAL => s"addDecimal{$text}"
                  case FieldType.DOUBLE => s"addDouble{$text}"
                  case FieldType.FLOAT => s"addFloat{$text}"
                  case FieldType.INT16 => s"addInt16{$text}"
                  case FieldType.INT32 => s"addInt32{$text}"
                  case FieldType.INT64 => s"addInt64{$text}"
                  case FieldType.STRING => s"addString{$text}"
                  case _ => s"addValue{$text}"
                }
              case _ => s"addValue(null)"
            }
        })
    }.mkString("\n")
    val initDef = mapping.getOrDefault("__init__", "").toString
    val foreachDef = mapping.getOrDefault("__foreach__", "").toString
    val varDef = varSb.toString
    s"""$funcType with Serializable {
  $init
  $initDef
  override def apply$applyDef = {
  val _recBuilder = Row.withSchema($schemaName)
  $varDef
  $foreachDef
  $mapDef
    _recBuilder.build()
  }
}"""
  }

  def getTestClass(keySchema:Schema, valueSchema:Schema, test:String): Class[(Row, Row) => Boolean] = {
    val src = scalaBooleanGen(keySchema, valueSchema, test)
    try {
      new ScalaCompiler().compile[(Row, Row) => Boolean](src)
    } catch {
      case e:Throwable => throw e
    }
  }

  private def scalaBooleanGen(keySchema:Schema, valueSchema:Schema, test:String): String = {
    val varDef = generateVars(valueSchema, "_valueRecord", "")
    s""" extends ((org.apache.beam.sdk.values.Row,org.apache.beam.sdk.values.Row) => Boolean) {
  $IMPORTS
  override def apply(_keyRecord:Row,_valueRecord:Row):Boolean = {
$varDef
$test
  }
}"""
  }

  private def generateVars(inSchema: Schema, varName:String, prefix:String):String = {
    inSchema.getFields.zipWithIndex.map{case (field, i) =>
      val fieldName = prefix + field.getName
      s"    lazy val $fieldName = $varName." + {
        field.getType match {
          case FieldType.BOOLEAN => s"getBoolean($i)"
          case FieldType.BYTE => s"getByte($i)"
          case FieldType.BYTES => s"getBytes($i)"
          case FieldType.DATETIME => s"getDateTime($i)"
          case FieldType.DECIMAL => s"getDecimal($i)"
          case FieldType.DOUBLE => s"getDouble($i)"
          case FieldType.FLOAT => s"getFloat($i)"
          case FieldType.INT16 => s"getInt16($i)"
          case FieldType.INT32 => s"getInt32($i)"
          case FieldType.INT64 => s"getInt64($i)"
          case FieldType.STRING => s"getString($i)"
          case fieldType => fieldType.getTypeName match {
              case Schema.TypeName.ARRAY =>
                "getArray[" + typeToJava(fieldType.getCollectionElementType) + s"]($i)"
              case Schema.TypeName.MAP =>
                "getMap[" + typeToJava(fieldType.getMapKeyType) + "," + typeToJava(fieldType.getMapValueType) + s"]($i)"
              case Schema.TypeName.ROW => s"getRow($i)"
              case unknown => throw new IllegalArgumentException("unknown type:" + unknown.toString)
            }
        }
      }
    }.mkString("\n")
  }

  private def typeToJava(beamType:Schema.FieldType):String = {
    beamType match {
      case FieldType.BOOLEAN => "java.lang.Boolean"
      case FieldType.BYTE => "java.lang.Byte"
      case FieldType.BYTES => "Array[java.lang.Byte]"
      case FieldType.DATETIME => "org.joda.time.ReadableInstant"
      case FieldType.DECIMAL => "java.math.BigDecimal"
      case FieldType.DOUBLE => "java.lang.Double"
      case FieldType.FLOAT => "java.lang.Float"
      case FieldType.INT16 => "java.lang.Short"
      case FieldType.INT32 => "java.lang.Integer"
      case FieldType.INT64 => "java.lang.Long"
      case FieldType.STRING => "java.lang.String"
      case fieldType => fieldType.getTypeName match {
          case Schema.TypeName.ARRAY =>
            "java.util.List[" + typeToJava(fieldType.getCollectionElementType) + "]"
          case Schema.TypeName.MAP =>
            "java.util.List[" + typeToJava(fieldType.getMapKeyType) + "," + typeToJava(fieldType.getMapValueType) + "]"
          case Schema.TypeName.ROW => "org.apache.beam.sdk.values.Row"
          case unknown => throw new IllegalArgumentException("unknown type:" + unknown)
        }
    }
  }
}
