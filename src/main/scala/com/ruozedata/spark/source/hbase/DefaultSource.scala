package com.ruozedata.spark.source.hbase

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.RelationProvider

/**
 * Created with IntelliJ IDEA.
 *
 * @Author: RJC0802
 * @Date: 2021/07/11/15:01
 * @Description:
 */
class DefaultSource extends RelationProvider{
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]) = {
    HBaseRelation(sqlContext,parameters)

  }
}
