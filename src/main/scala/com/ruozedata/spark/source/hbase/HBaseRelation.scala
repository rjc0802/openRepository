package com.ruozedata.spark.source.hbase


import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}

import scala.collection.mutable.ArrayBuffer

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: RJC0802
 * @Date: 2021/07/11/15:03
 * @Description:
 */
case class HBaseRelation(sqlContext: SQLContext, parameters: Map[String, String])
  extends BaseRelation with TableScan{
  val hbaseTableName = parameters.getOrElse("habase.table.name", sys.error("hbase.table.name is required..."))
  val hbaseColumns = parameters.getOrElse("hbase.table.columns", sys.error("hbase.table.columns is required ..."))
  val sparkColumns = parameters.getOrElse("spark.table.columns", sys.error("spark.table.columns is required ..."))
  val hbaseSchemaFields = HbaseSourceUtils.extractHbaseSchema(hbaseColumns)
  val sparkSchemaFields = HbaseSourceUtils.extractSparkSchema(sparkColumns)
  val schemaMapping = HbaseSourceUtils.tableSchemaMapping(hbaseSchemaFields, sparkSchemaFields)
  val hbaseSchemaIncludeType = HbaseSourceUtils.feedHbaseType(schemaMapping)
  val hbaseSearchFields = HbaseSourceUtils.getHbaseSearchSchema(hbaseSchemaIncludeType)

  override def schema = {
    val structFields = hbaseSchemaIncludeType.map(field => {
      val sparkSchema = schemaMapping.getOrElse(HbaseSchemaField(field.fieldName, ""), sys.error(s"this${field} can not mapping spark schema！"))
      val relationType = field.fieldType.toLowerCase match {
        case "string" => SchemaType(StringType, nullable = false)
        case "int" => SchemaType(IntegerType, nullable = false)
        case "long" => SchemaType(LongType, nullable = false)
        case "double" => SchemaType(DoubleType, nullable = false)
      }
      StructField(sparkSchema.fieldName, relationType.dataType, relationType.nullable)
    })
    StructType(structFields)
  }

  override def buildScan() = {
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM,"cdh516server")
    hbaseConf.set(HConstants.ZOOKEEPER_CLIENT_PORT,"2181")
    hbaseConf.set(TableInputFormat.INPUT_TABLE,hbaseTableName)
    hbaseConf.set(TableInputFormat.SCAN_COLUMNS,hbaseSearchFields)
    hbaseConf.set(TableInputFormat.SCAN_CACHEDROWS, "10000") //指定查询时候，缓存多少数据
    hbaseConf.set(TableInputFormat.SHUFFLE_MAPS, "1000")

    val hbaseRdd = sqlContext.sparkContext.newAPIHadoopRDD(
      hbaseConf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])


    val finalResult = hbaseRdd.map(_._2).map(result => {
      val buffer = new ArrayBuffer[Any]()

      hbaseSchemaIncludeType.foreach(field => {
        val results = ResolveUtils.resolve(field, result)
        buffer += results
      })
      Row.fromSeq(buffer)
    })
    finalResult
  }
}
