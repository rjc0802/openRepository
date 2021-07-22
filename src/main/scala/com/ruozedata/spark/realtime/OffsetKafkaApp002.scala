package com.ruozedata.spark.realtime

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}
import java.lang



object OffsetKafkaApp002 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName(this.getClass.getCanonicalName)
      .setMaster("local")

    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "cdh516server:9092"
      , "group.id" -> "group001"
      , "key.deserializer" -> classOf[StringDeserializer]
      , "value.deserializer" -> classOf[StringDeserializer]
      , "auto.offset.reset" -> "earliest"
      , "enable.auto.commit" -> (false: lang.Boolean)
    )
    val topics = Array("topic001")

    val kafkaStream = KafkaUtils.createDirectStream(ssc
      , PreferConsistent, Subscribe[String, String](topics, kafkaParams))

    var offsetRanges:Array[OffsetRange] = null

    val transformStream = kafkaStream.transform(rdd => {
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })

    val window = transformStream.window(Seconds(5), Seconds(5))
    window.flatMap(_.value()
      .split(","))
      .map((_,1))
      .mapWithState(StateSpec.function(mappingFunction2))

    window.foreachRDD(rdd=>{
      if(!rdd.isEmpty()){
        rdd.foreach(println)
        kafkaStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }else{
        println("该批次没有数据!")
      }
    })



  }

  val mappingFunction2=(word:String,value:Option[Int],state:State[Int])=>{
    if(state.isTimingOut()){
      println("...isTimingOut")
    }else{
      val sum = value.getOrElse(0) + state.getOption().getOrElse(0)
      val tmp = (word, sum)
      state.update(sum)
      tmp
    }
  }


}
