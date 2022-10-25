package org.customSink

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.data.{AccountInfo, CustomUser}
import org.slf4j.{Logger, LoggerFactory}

import java.sql._
import scala.collection.mutable.ListBuffer
//
class CustomMysqlJdbcSink2 extends RichSinkFunction[ListBuffer[CustomUser]] {
  //连接、定义预编译器
  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _
  val info:AccountInfo = AccountInfo()
  private val LOG:Logger = LoggerFactory.getLogger(classOf[CustomMysqlJdbcSink2])

  //初始化 建立
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    Class.forName(info.JDBC_DRIVER)
    conn = DriverManager.getConnection(
      info.sinkJdbcUrl,
      info.sinkJdbcUserName,
      info.sinkJdbcPassword
    )
    insertStmt = conn.prepareStatement(
      """insert into flinkSinkTest
        |(id
        |,name
        |,age) values (?,?,?)""".stripMargin)
    updateStmt = conn.prepareStatement("update flinkSinkTest set name=? where id=?")
  }

  //调用连接执行sql
  override def invoke(value: ListBuffer[CustomUser], context: SinkFunction.Context): Unit = {
    //执行更新语句
//    for (eachUser <- value){
//      updateStmt.setString(1, eachUser.name)
//      updateStmt.setInt(2, eachUser.id)
//      updateStmt.addBatch()
//    }
//    updateStmt.executeBatch()
//    updateStmt.clearBatch()
//
//    //如果update没有查到数据，执行插入语句
//    if (updateStmt.getUpdateCount == 0) {
      for (eachUser <- value) {
        insertStmt.setInt(1, eachUser.id)
        insertStmt.setString(2, eachUser.name)
        insertStmt.setInt(3, eachUser.age)
        insertStmt.addBatch()
        LOG.info("value: "+eachUser.toString)
      }
      insertStmt.executeBatch()
      insertStmt.clearBatch()
    }
//  }

  //关闭时 做清理工作
  override def close(): Unit = {
    conn.close()
    insertStmt.close()
    updateStmt.close()
  }
}