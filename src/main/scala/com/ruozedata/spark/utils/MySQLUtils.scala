package com.ruozedata.spark.utils


import java.sql.{Connection, DriverManager}



object MySQLUtils {
  // 定义链接方法
  def getConnection()={
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://cdh516server:3306/test?useUnicode=true&characterEncoding=UTF-8","root","14529000")
  }

  // 用完关闭链接
  def closeConnection(connection: Connection): Unit ={
    if(null!=connection){
      connection.close()
    }
  }

}
