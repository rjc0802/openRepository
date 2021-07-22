package com.ruozedata.spark.source

import org.apache.spark.sql.types.DataType

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: RJC0802
 * @Date: 2021/07/11/15:08
 * @Description:
 */
package object hbase {
  abstract class SchemaField extends Serializable
  case class SchemaType(dataType:DataType,nullable:Boolean)
  case class HbaseSchemaField(fieldName:String,fieldType:String) extends SchemaField
  case class SparkSchemaField(fieldName:String,fieldType:String) extends SchemaField
}
