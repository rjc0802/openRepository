package com.ruozedata.spark.topn.rdd

import org.apache.spark.Partitioner
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.util.Random

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: RJC0802
 * @Date: 2021/07/09/8:06
 * @Description:
 */
object TopNApp001 extends Serializable {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getCanonicalName)
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._

    val df = spark.read.orc("data/orc")
    val process = df.as[SiteUrl]
      .rdd.map(x => {
      val prefixSite = Random.nextInt(9).toString + "_" + x.site
      (SiteUrl(prefixSite, x.url), 1)
    }).cache()

    val sites = process.map(x => (x._1.site)).distinct().collect()
    //    sites.foreach(println)
    //    println("....................")

    val rddReduceOne = process.reduceByKey(new MyPartitioner(sites), _ + _)
      .map(x => {
        val splits = x._1.site.split("_")
        (SiteUrl(splits(1), x._1.url), x._2)
      })

    rddReduceOne.foreach(println)
    println("....................")


    //    implicit  object obj extends Ordering[((String, String), Int)] {
    //      override def compare(x: ((String, String), Int), y: ((String, String), Int)): Int =
    //        -(x._2-y._2)
    //    }

    val sites1 = rddReduceOne.map(x => x._1.site).distinct().collect()
    rddReduceOne.reduceByKey(new MyPartitioner(sites1), _ + _)
      .mapPartitions(partition => {


        val treeSet = new mutable.TreeSet[((String, String), Int)]()(Ordering[Int].on[((String, String), Int)](x => -x._2))


        partition.foreach(x => {
          //          treeSet.add((x._1.site,x._1.url),x._2)
          treeSet.add((x._1.site, x._1.url), x._2)

          //          if(treeSet.size>2){
          //            treeSet.remove(treeSet.last)
          //          }
        })
        treeSet.iterator
      }).foreach(println)


    spark.stop()
  }

  class MyPartitioner(sites: Array[String]) extends Partitioner {
    val sitesMap = mutable.HashMap[String, Int]()

    for (i <- 0 until sites.length) {
      sitesMap(sites(i)) = i
    }

    override def numPartitions = sites.length

    override def getPartition(key: Any) = {
      val site = key.asInstanceOf[SiteUrl].site
      sitesMap(site)
    }
  }

  case class SiteUrl(site: String, url: String)

}
