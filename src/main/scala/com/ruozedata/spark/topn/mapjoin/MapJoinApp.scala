package com.ruozedata.spark.topn.mapjoin

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: RJC0802
 * @Date: 2021/07/10/13:02
 * @Description:
 */
object MapJoinApp {
  def main(args: Array[String]): Unit = {
//    val sparkConf = new SparkConf()
//      .setAppName(this.getClass.getCanonicalName)
//      .setMaster("local[2]")
//
//    val sc = new SparkContext(sparkConf)
//
//    val rddPeopleInfo = sc
//      .parallelize(Array(("110", "zs"), ("220", "ls")))
//      .collectAsMap()
//
//    val peopleBC = sc.broadcast(rddPeopleInfo)
//
//    val peopleDetail = sc.parallelize(Array(
//      ("110", "school1", 1),
//      ("111", "school2", 2)
//    )).map(x => (x._1, x))
//
//    val resRdd = peopleDetail.mapPartitions(partition => {
//      val broadCastPeople = peopleBC.value
//      for ((key, value) <- partition if broadCastPeople.contains(key))
//        yield (key, broadCastPeople.getOrElse(key, ""), value._2)
//    })

    val spark = SparkSession
      .builder()
      .master("local")
      .appName(this.getClass.getCanonicalName)
      .getOrCreate()

    import spark.implicits._

    val rddA = spark.sparkContext.parallelize(Array(("110", "zs"), ("220", "ls")))
    val rddB = spark.sparkContext.parallelize(Array(
      ("110", "school1", 1),
      ("111", "school2", 2)
    ))

    val dataFrameA = rddA.toDF("id", "course")
    val dataFrameB = rddB.toDF("id", "schoolname", "num")
    dataFrameA.createOrReplaceTempView("tableA")
    dataFrameB.createOrReplaceTempView("tableB")

    val dfA = spark.sql("select id,course from tableA")
    val dfB = spark.sql("select id,schoolname,num from tableB")

    val bcA = spark.sparkContext.broadcast(dfA)
    val joinRes = dfB.join(bcA.value, dfB("id") === dfA("id"), "left")

    joinRes.show(false)



    Thread.sleep(Int.MaxValue)
    spark.stop()
  }
}
