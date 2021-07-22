package com.ruozedata.spark.topn.rdd

import org.apache.spark.Partitioner
import org.apache.spark.sql.SparkSession

import java.util.Random
import scala.collection.mutable

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: RJC0802
 * @Date: 2021/07/09/12:42
 * @Description:
 */
object TopNApp002 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getCanonicalName)
      .master("local")
      .getOrCreate()

    import spark.implicits._
    val process = spark.read.orc("data/orc")
      .as[SiteUrl].rdd
      .map(x => {
        val random = new Random()
        val prefixSite = random.nextInt(9) + "_" + x.site
        ((prefixSite, x.url), 1)
      }).cache()

    val prefixSites = process.map(x => x._1._1).distinct().collect()

    val processNOPrefix = process.reduceByKey(new MyPartitioner1(prefixSites), _ + _)
      .map(x => {
        val splits = x._1._1.split("_")
        ((splits(1), x._1._2), x._2)
      })


    val sites = processNOPrefix.map(x => x._1._1).distinct().collect()
    processNOPrefix.reduceByKey(new MyPartitioner1(sites), _ + _)
      .mapPartitions(partition => {
        val treeSet = new mutable.TreeSet[((String, String), Int)]()(Ordering[Int].on[((String, String), Int)](x => -x._2))
        partition.foreach(rdd => {
          treeSet.add(rdd)
          if (treeSet.size > 2) {
            treeSet.remove(treeSet.last)
          }
        })
        treeSet.iterator
      }).foreach(println)


    Thread.sleep(Int.MaxValue)

    spark.stop()
  }

  case class SiteUrl(site: String, url: String)

  class MyPartitioner1(prefixSites: Array[String]) extends Partitioner {
    private val sitePartitons = new mutable.HashMap[String, Int]()
    for (i <- 0 until prefixSites.length) {
      sitePartitons(prefixSites(i)) = i
    }

    override def numPartitions = prefixSites.length

    override def getPartition(key: Any) = {
      val site = key.asInstanceOf[(String, String)]._1
      sitePartitons(site)
    }
  }

}
