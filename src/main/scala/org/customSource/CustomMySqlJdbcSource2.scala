package org.customSource

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.data.{AccountInfo, CustomUser}

import scala.collection.mutable.ListBuffer
//import org.slf4j.LoggerFactory

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
//多条插入
class CustomMySqlJdbcSource2 extends RichSourceFunction[ListBuffer[CustomUser]] {
  var conn: Connection = _
  var stat: PreparedStatement = _
  val info: AccountInfo = AccountInfo()

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    conn = DriverManager.getConnection(
      info.sourceJdbcUrl,
      info.sourceJdbcUserName,
      info.sourceJdbcPassword)
    //编写查询的数据sql
    stat = conn.prepareStatement("select * from flinkSourceTest")
  }

  override def run(ctx: SourceFunction.SourceContext[ListBuffer[CustomUser]]): Unit = {
    /**
     * 使用jdbc读取mysql的数据，将读取到的数据发送到下游
     */
    Class.forName("com.mysql.cj.jdbc.Driver")

    //执行查询
    val result: ResultSet = stat.executeQuery()
    //    val logger = LoggerFactory.getLogger(getClass())
    //解析数据
    val resultBatch = scala.collection.mutable.ListBuffer.empty[CustomUser]
    while (result.next()) {
      val id: Int = result.getInt("id")
      val name: String = result.getString("name")
      val age: Int = result.getInt("age")
      //将每一条数据发送到下游
      val new_user = new CustomUser(id, name, age)
      if (resultBatch.size<100){
        resultBatch+= new_user
      }
      else{
        ctx.collect(resultBatch)
        resultBatch.clear()
      }
    }
    ctx.collect(resultBatch)
    resultBatch.clear()
  }

  //任务被取消的时候执行，一般用于回收资源
  override def cancel(): Unit = {
    try {
      if (conn != null) {
        conn.close()
      }
      if (stat != null) {
        stat.close()
      }
    } catch {
      case e: Exception => print(e)
    }
  }
}