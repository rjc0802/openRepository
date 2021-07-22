package com.ruozedata.spark.utils

import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: RJC0802
 * @Date: 2021/07/10/21:35
 * @Description:
 */
object HBaseUtils {
  def getConnection(zk:String,port:Int):Connection={
    val conf = HBaseConfiguration.create()
    conf.set(HConstants.ZOOKEEPER_QUORUM,zk)
    conf.set(HConstants.ZOOKEEPER_CLIENT_PORT,port+"")
    ConnectionFactory.createConnection(conf)
  }
}
