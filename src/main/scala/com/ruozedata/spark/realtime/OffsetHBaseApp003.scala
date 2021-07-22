package com.ruozedata.spark.realtime

import com.ruozedata.spark.utils.HBaseUtils
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: RJC0802
 * @Date: 2021/07/10/21:35
 * @Description:
 */
object OffsetHBaseApp003 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName(this.getClass.getCanonicalName)

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val groupId = "groupId001"
    val topic = "hbaseoffset"
    val topics = Array(topic)

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "cdh516server:9092,cdh516client1:9092,cdh516client2:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" ->groupId ,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val kafkaStream = KafkaUtils.createDirectStream(
      ssc
      , PreferConsistent
      , Subscribe[String, String](topics, kafkaParams))

    kafkaStream.foreachRDD(rdd=>{
      if(!rdd.isEmpty()) {
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd.map(_.value())
          .map(x => {
            val splits = x.split("\t")
            if (splits.length == 3) {
              Sales(splits(0).trim, splits(1).trim, splits(2).trim.toDouble)
            } else {
              Sales("0", "0", 0.0)
            }
          }).filter(_.id != "0")
          .foreachPartition(partition => {
            if (partition.nonEmpty) {
              val partitionId = TaskContext.getPartitionId()
              val offset = offsetRanges(partitionId)

              val connection = HBaseUtils.getConnection("cdh516server", 2181)
              val table = connection.getTable(TableName.valueOf("ruozedata_hbase_offset"))

              val puts = new util.ArrayList[Put]()
              partition.foreach(x => {
                val put = new Put(Bytes.toBytes(x.id))
                put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("name"), Bytes.toBytes(x.name))
                put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("money"), Bytes.toBytes(x.money))

                // 分区没有数据了 插入offset
                if (!partition.hasNext) {
                  put.addColumn(Bytes.toBytes("o"), Bytes.toBytes("groupid"), Bytes.toBytes(groupId))
                  put.addColumn(Bytes.toBytes("o"), Bytes.toBytes("topic"), Bytes.toBytes(offset.topic))
                  put.addColumn(Bytes.toBytes("o"), Bytes.toBytes("partition"), Bytes.toBytes(offset.partition + ""))
                  put.addColumn(Bytes.toBytes("o"), Bytes.toBytes("offset"), Bytes.toBytes(offset.untilOffset + ""))
                }
                puts.add(put)
              })
              table.put(puts)
              table.close()
              connection.close()
            }
          })
      }else{
        println("本批次没有数据")
      }
    })
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

  case class Sales(id:String,name:String,money:Double)
}
