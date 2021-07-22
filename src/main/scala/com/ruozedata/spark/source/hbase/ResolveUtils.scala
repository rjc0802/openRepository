package com.ruozedata.spark.source.hbase

import org.apache.hadoop.hbase.client.Result


object ResolveUtils {

  def resolveRowKey(result: Result, fieldType: String) = {
    val rowKey = fieldType.toLowerCase() match {
      case "string" => result.getRow.map(_.toChar).mkString
      case "int" => result.getRow.map(_.toChar).mkString.toInt
      case "long" => result.getRow.map(_.toChar).mkString.toLong
      case "double" => result.getRow.map(_.toChar).mkString.toDouble
      case "float" => result.getRow.map(_.toChar).mkString.toFloat
    }
    rowKey
  }

  def resolveColumn(result: Result, cfName: String, colName: String, fieldType: String) = {
    val column = result.containsColumn(cfName.getBytes, colName.getBytes) match {
      case true =>
        fieldType.toLowerCase match {
          case "string" => result.getValue(cfName.getBytes, colName.getBytes).map(_.toChar).mkString
          case "int" => result.getValue(cfName.getBytes, colName.getBytes).map(_.toChar).mkString.toInt
          case "long" => result.getValue(cfName.getBytes, colName.getBytes).map(_.toChar).mkString.toLong
          case "double" => result.getValue(cfName.getBytes, colName.getBytes).map(_.toChar).mkString.toDouble
          case "float" => result.getValue(cfName.getBytes, colName.getBytes).map(_.toChar).mkString.toFloat
        }
      case _ =>
        fieldType.toLowerCase() match {
          case "string" => ""
          case "int" => 0
          case "long" => 0
          case "double" => 0l
          case "float" => 0.0
        }
    }
    column
  }


  def resolve(hbaseSchemaField: HbaseSchemaField, result: Result) = {
    val cfAndCol = hbaseSchemaField.fieldName.split(":")
    val cfName = cfAndCol(0)
    val colName = cfAndCol(1)
    var resolveResult:Any=null
    if(cfName=="" && colName=="key"){
      resolveResult = resolveRowKey(result, hbaseSchemaField.fieldType)
    }else{
      resolveColumn(result,cfName,colName,hbaseSchemaField.fieldType)
    }
    resolveResult
  }
}
