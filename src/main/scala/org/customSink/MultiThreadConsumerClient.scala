package org.customSink

import org.data.{accountInfo, user}
import org.slf4j.LoggerFactory

import java.sql.DriverManager
import java.util.concurrent.{CyclicBarrier, LinkedBlockingQueue, TimeUnit}

private class MultiThreadConsumerClient(bufferQueue: LinkedBlockingQueue[user], barrier: CyclicBarrier) extends Runnable {
  val logger = LoggerFactory.getLogger(getClass())

  override def run(): Unit = {
    while (true) {
      // 从 bufferQueue 的队首消费数据
      logger.info("loading data in "+bufferQueue.hashCode())
      val entity = bufferQueue.poll(50, TimeUnit.MILLISECONDS)
      if (entity != null) {
        // 执行 client 消费数据的逻辑
        doSomething(entity)
      }
      else {
        if (barrier.getNumberWaiting > 0) {
          println("MultiThreadConsumerClient 执行 flush, " +
            "当前 wait 的线程数：" + barrier.getNumberWaiting())
          barrier.await()
        }
      }
    }

    def doSomething(value: user): Unit = {
      // client 积攒批次并调用第三方 api
      logger.info("entityArray: "+value.toString())
      val info:accountInfo = new accountInfo()
      Class.forName("com.mysql.cj.jdbc.Driver")
      val conn = DriverManager.getConnection(
        info.sinkJdbcUrl,
        info.sinkJdbcUserName,
        info.sinkJdbcPassword
      )
      val insertStmt = conn.prepareStatement(
        """insert into flinkSinkTest
          |(id
          |,name
          |,age
          ) values (?,?,?)""".stripMargin)
      val updateStmt = conn.prepareStatement("update flinkSinkTest set name=? where id=?")
      //执行更新语句
      updateStmt.setString(1, value.name)
      updateStmt.setInt(2, value.id)
//      println(s"updateStmt: $updateStmt")
      updateStmt.execute()

      //如果update没有查到数据，执行插入语句
      if (updateStmt.getUpdateCount == 0) {
        insertStmt.setInt(1, value.id)
        insertStmt.setString(2, value.name)
        insertStmt.setInt(3, value.age)
//        println(s"insertStmt: $insertStmt")
        insertStmt.execute()
      }
      conn.close()
      insertStmt.close()
      updateStmt.close()
    }
  }
}
