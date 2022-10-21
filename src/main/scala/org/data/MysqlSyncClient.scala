package org.data

import java.sql._

class MysqlSyncClient[customUser] {
  val info: AccountInfo = AccountInfo()
  var connection: Connection = _

  try {
    Class.forName(info.JDBC_DRIVER)
  } catch {
    case e: ClassNotFoundException => println("Driver not found!" + e.getMessage)
  }
  try {
    connection = DriverManager.getConnection(info.sourceJdbcUrl, info.sourceJdbcUserName, info.sourceJdbcPassword)
  } catch {
    case e: SQLException => println("init connection failed!" + e.getMessage)
  }

  def query(t: customUser):String = {
    val info = t.toString.split(",")
    val info1 = info(0)
    val info2 = info(1)
    val info3 = info(2)
    val sqlQuery: String = s"insert into flinkSinkTest values ($info1,'$info2',$info3)"
    println(s"sqlQuery: $sqlQuery")
    val statement: Statement = connection.createStatement()
    // https://stackoverflow.com/questions/25745094/getting-resultset-from-insert-statement
    val result: Int = statement.executeUpdate(sqlQuery)
    val ans: String = s"insert $result row values (${info.mkString(",")})"
    ans
  }

  def close(): Unit = {
    try {
      if (connection != null) {
        connection.close()
      }
    } catch {
      case e: SQLException => println("close connection failed!" + e.getMessage)
    }
  }
}
