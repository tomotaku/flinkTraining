package org.customSink

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.data.{AccountInfo, CustomUser}
import org.slf4j.{Logger, LoggerFactory}

import java.sql._

class CustomMysqlJdbcSink extends RichSinkFunction[CustomUser] {
  //连接、定义预编译器
  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _
  val info:AccountInfo = AccountInfo()
  private val LOG:Logger = LoggerFactory.getLogger(classOf[CustomMysqlJdbcSink])

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
  override def invoke(value: CustomUser, context: SinkFunction.Context): Unit = {
    //执行更新语句
    updateStmt.setString(1, value.name)
    updateStmt.setInt(2, value.id)
    updateStmt.execute()

    //如果update没有查到数据，执行插入语句
    if (updateStmt.getUpdateCount == 0) {
      insertStmt.setInt(1, value.id)
      insertStmt.setString(2, value.name)
      insertStmt.setInt(3, value.age)
      insertStmt.execute()
    }
    LOG.info("value: "+value.toString)
  }

  //关闭时 做清理工作
  override def close(): Unit = {
    conn.close()
    insertStmt.close()
    updateStmt.close()
  }
}