package com.ruozedata.spark.topn.df


import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: RJC0802
 * @Date: 2021/07/09/14:03
 * @Description:
 */
object TopNApp003 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getCanonicalName)
      .master("local")
      .getOrCreate()

    import spark.implicits._
//    val processDf = spark.read.orc("data/orc")
//    processDf.createOrReplaceTempView("process")
//    spark.sql("select * from process limit 10").show(false)
//    val value = spark.read.textFile("data/site.log")
//    value.printSchema()
    val rddRow = spark.sparkContext.textFile("data/site.log")
      .map(line => {
        val splits = line.split(" ")
        Row(splits(0), splits(1))
      })


    val structType = new StructType(Array(
      StructField("site", StringType, false)
      , StructField("url", StringType, false)
    ))
    val rddFrame = spark.createDataFrame(rddRow, structType)

    rddFrame.createOrReplaceTempView("process")
//    spark.sql("select * from process ").show(false)

    spark.sql(
      """
        |select
        | *
        |from
        |(
        |select
        |   tmp2.site
        |  ,tmp2.url
        |  ,tmp2.t_cnt
        |  ,row_number() over(partition by tmp2.site order by tmp2.t_cnt desc) as rn
        |from
        |(
        |select split(tmp.prefix_site,"_")[1]  as site,tmp.url,sum(tmp.cnt) as t_cnt
        |from
        |(
        |select concat(floor(rand()*3),"_",site) as prefix_site,url,count(1) as cnt
        |from process
        |group by prefix_site,url
        |) tmp
        |group by site,tmp.url
        |)tmp2
        |)tmp3
        |where tmp3.rn <= 2
        |
        |""".stripMargin).show(false)



      Thread.sleep(Int.MaxValue)
    spark.stop()
  }
}
