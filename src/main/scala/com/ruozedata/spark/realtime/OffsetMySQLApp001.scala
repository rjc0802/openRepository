package com.ruozedata.spark.realtime

import com.ruozedata.spark.utils.MySQLUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.lang
import java.sql.{Connection, PreparedStatement}
import scala.collection.mutable

object OffsetMySQLApp001 {
  def main(args: Array[String]): Unit = {
      if(args.length!=4){
        System.err.println(
          """
            |Usage:OffseApp02 <batch><groupId><brokers><topic>
            |<batch>:Spark流处理作业运行的时间间隔
            |<groupId>:消费组编号
            |<brokers>:Kafka集群地址
            |<topic>:消费的Topic名称
            |""".stripMargin)
        System.exit(1)
      }

      val Array(batch,groupId,brokers,topic) = args
      val sparkConf = new SparkConf()
        .setAppName(this.getClass.getCanonicalName)
        .setMaster("local[2]")

      val ssc = new StreamingContext(sparkConf, Seconds(batch.toInt))

      val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> brokers.toString
        , "group.id" -> groupId
        , "key.deserializer" -> classOf[StringDeserializer]
        , "value.deserializer" -> classOf[StringDeserializer]
        , "auto.offset.reset" -> "earliest"
        , "enable.autocommit" -> (false: lang.Boolean)
      )

      val topics = Array(topic)

      //读取维护在MySQL的offsets
      val offsets = new mutable.HashMap[TopicPartition,Long]()
      var connection:Connection = null
      var pstmt:PreparedStatement=null

      // sql查询语句
      val sql = {
      s"""
       |select topic,group_id,partition,offset
       |from ruozedata_offsets_storage
       |where group_id =? and topic=?
       |""".stripMargin

        try{
          connection = MySQLUtils.getConnection()
          pstmt = connection.prepareStatement(sql)
          pstmt.setString(1,groupId)
          pstmt.setString(2,topic)
          val rs = pstmt.executeQuery()
          // 遍历查询结果
          while (rs.next()){
            val topic = rs.getString("topic")
            val partition = rs.getInt("part")
            val offset = rs.getLong("offset")
            // 构建TopicPartiton对象
            val topicPartition = new TopicPartition(topic, partition)
            // 将offset 和 topicPartition封装到Map
            offsets(topicPartition)=offset
          }
        }catch {
          case e:Exception=>e.printStackTrace()
        }finally {
          if(pstmt!=null){
            pstmt.close()
          }
          if(connection!=null){
            connection.close()
          }
        }
        // spark对接kafka的入口点 kafkaUtils
        val kafkaStream = KafkaUtils.createDirectStream[String, String](
          ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams, offsets)
        )

        // 获取offsetRanges
        kafkaStream.foreachRDD(rdd=>{
          if(!rdd.isEmpty()){
            val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            val result = rdd.flatMap(_.value().split(","))
              .map((_, 1))
              .reduceByKey(_ + _)
              .collect()

            result.foreach(println)

            var connection:Connection =null
            var pstmt1:PreparedStatement = null
            var pstmt2:PreparedStatement = null
            val sql1 = s"insert into ruozedata_wc values(?,?) on duplicate key update cnt =cnt +?;"
            val sql2 =
              s"""insert into ruozedata_offsets_storage(
                 |topic,group_id,part,offset) values(?,?,?,?)
                 |on duplicate key update offset = ?;""".stripMargin
            // 开启事务
            try{
              val connection = MySQLUtils.getConnection()
              connection.setAutoCommit(false)
              pstmt1=connection.prepareStatement(sql1)
              pstmt2=connection.prepareStatement(sql2)
              for(ele <- result){
                pstmt1.setString(1,ele._1)
                pstmt1.setInt(2,ele._2)
                pstmt1.setInt(3,ele._2)    // cnt
                pstmt1.addBatch()
              }
              for (ele <- offsetRanges) {
                pstmt2.setString(1,ele.topic)
                pstmt2.setString(2,groupId)
                pstmt2.setInt(3,ele.partition)
                pstmt2.setLong(4,ele.untilOffset)
                pstmt2.addBatch()
              }

              pstmt1.executeBatch()
              pstmt2.executeBatch()
              connection.commit()
            }catch {
              case e:Exception => e.printStackTrace()
              connection.rollback()
            }finally {
              if(pstmt1!=null){
                pstmt1.close()
              }
              if(pstmt2!=null){
                pstmt1.close()
              }
              if(connection!=null){
                connection.close()
              }
            }
          }else{
            println("该批次没有数据")
           }
        })


        ssc.start()
        ssc.awaitTermination()
        ssc.stop()
    }

  }


}
