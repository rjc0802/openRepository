package com.ruozedata.spark.topn.mapjoin

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable
import scala.util.Random

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: RJC0802
 * @Date: 2021/07/09/16:44
 * @Description:
 */
object JoinApp {


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getCanonicalName)
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._

    val rddA = spark.sparkContext.textFile("data/site.log")
      .map(line=>{
        val splits = line.split(" ")
        Row(splits(0),splits(1))
      })
    val rddB = spark.sparkContext.textFile("data/fileB/site.log")
      .map(line=>{
        val splits = line.split(" ")
        Row(splits(0),splits(1))
      })

    val structType =  StructType(Array(
      StructField("site", StringType, false)
      , StructField("url", StringType, false)
    ))

    val dataFrameA = spark.createDataFrame(rddA, structType)
    val dataFrameB = spark.createDataFrame(rddB, structType)

    val addRandomPrefix=(site:String)=>s"${Random.nextInt(10)}_${site}"
    val addFixPrefix=(i:Int,site:String)=>s"${i}_${site}"

    spark.udf.register("addRandomPrefix",addRandomPrefix)
    spark.udf.register("addFixPrefix",addFixPrefix)

    def capacity(dataFrameB: DataFrame):DataFrame = {
      val emptyRdd = spark.sparkContext.emptyRDD[Row]
      var emptyDataFrame = spark.createDataFrame(emptyRdd, dataFrameB.schema)
      // 循环1-10 给 emptyDataFrame添加固定前缀
      for(i <- 0 until 10){
        emptyDataFrame = emptyDataFrame.union(dataFrameB.selectExpr(s"addFixPrefix(${i},site)  site","url"))
      }
      emptyDataFrame
    }

    // 添加随机前缀
    dataFrameA.selectExpr("addRandomPrefix(site) site","url").createOrReplaceTempView("tableA")

    // 扩容dataFrameB
    capacity(dataFrameB).createOrReplaceTempView("tableB")
//    spark.sql("select * from tableB").show()

//    spark.sql("select * from tableB").show()
    val resFrame = spark.sql(
      """
        |select A.site,A.url
        |from tableA A
        |join tableB B
        |on A.site = B.site and A.url =B.url
        |group by A.site,A.url
        |""".stripMargin)

    val mapRdd = resFrame.rdd
      .map(x => {
        val site = x.getAs[String]("site").split("_")(1)
        val url = x.getAs[String]("url")
        (SiteUrl(site, url), 1)
      })

    val siteslist = mapRdd.map(x => x._1.site).distinct().collect()
    mapRdd.reduceByKey(new MyPartitioner2(siteslist),_+_)
      .mapPartitions(partiton=>{
        val treeSet = new mutable.TreeSet[((String, String), Int)]()(Ordering[Int].on[((String,String),Int)](x=> -x._2))
        partiton.foreach(rdd=>{
          treeSet.add((rdd._1.site,rdd._1.url),rdd._2)

          if(treeSet.size>2){
            treeSet.remove(treeSet.last)
          }
        })
        treeSet.iterator
      }).foreach(println)

    Thread.sleep(Int.MaxValue)

    spark.stop()
  }


  case class SiteUrl(site:String,url:String)

  class MyPartitioner2(siteList: Array[String]) extends Partitioner {
    val sitePartition = new mutable.HashMap[String, Int]()
    for(i <- 0 until siteList.length){
      sitePartition(siteList(i))=i
    }
    override def numPartitions = siteList.length

    override def getPartition(key: Any) = {
      val site = key.asInstanceOf[SiteUrl].site
      sitePartition(site)
    }
  }

}
