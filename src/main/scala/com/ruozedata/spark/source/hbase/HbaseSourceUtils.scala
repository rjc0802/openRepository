package com.ruozedata.spark.source.hbase

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: RJC0802
 * @Date: 2021/07/11/15:15
 * @Description:
 */
object HbaseSourceUtils {

  def isRowKey(hbaseField: HbaseSchemaField) = {
    val splits = hbaseField.fieldName.split(":")
    val cf = splits(0)
    val col = splits(1)
    if(cf==" "&& col=="key"){
      true
    }else{
      false
    }
  }


  def getHbaseSearchSchema(hbaseSchemaIncludeType: Array[HbaseSchemaField]) = {
    val strBuff = ArrayBuffer[String]()
    hbaseSchemaIncludeType.foreach(hbaseField=>{
      if(!isRowKey(hbaseField)){
        strBuff+=hbaseField.fieldName
      }
    })
    strBuff.mkString(" ")
  }

  def feedHbaseType(schemaMapping: mutable.LinkedHashMap[HbaseSchemaField, SparkSchemaField]) = {
    val finalHbaseSchema = schemaMapping.map(
      schemaMap => schemaMap match {
        case (hbaseSchemaField, sparkSchemaField) => hbaseSchemaField.copy(fieldType = sparkSchemaField.fieldType)
      }
    )
    finalHbaseSchema.toArray
  }

  def tableSchemaMapping(hbaseSchemaFields: Array[HbaseSchemaField], sparkSchemaFields: Array[SparkSchemaField]) = {
    if(hbaseSchemaFields.length!=sparkSchemaFields.length){
      sys.error("hbaseSchema is not equal to sparkSchema ...")
    }
    val zipSchemas = hbaseSchemaFields.zip(sparkSchemaFields)
    val schemaMap = new mutable.LinkedHashMap[HbaseSchemaField, SparkSchemaField]
    for (zipschema <- zipSchemas) {
      schemaMap.put(zipschema._1,zipschema._2)
    }
    schemaMap
  }

  def extractSparkSchema(sparkColumns: String) = {
    val fieldsStr = sparkColumns.trim.drop(1).dropRight(1)
    val fieldsArray = fieldsStr.split(",").map(_.trim)
    val sparkSchemaFields = fieldsArray.map(fieldNameAndType => {
      val fieldType = fieldNameAndType.split(" ")
      SparkSchemaField(fieldType(0), fieldType(1))
    })
    sparkSchemaFields
  }

  def extractHbaseSchema(hbaseColumns: String) = {
    val fieldsStr = hbaseColumns.trim.drop(1).dropRight(1)
    val fieldsArray = fieldsStr.split(",").map(_.trim)
    val hbaseSchemaFields = fieldsArray.map(fieldName => {
      HbaseSchemaField(fieldName, "")
    })
    hbaseSchemaFields
  }

}
