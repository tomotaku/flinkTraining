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
    val info = t.toString
    val sqlQuery: String = s"insert table ??? values ($info)"
    val statement: Statement = connection.createStatement()
    // https://stackoverflow.com/questions/25745094/getting-resultset-from-insert-statement
    val result: Int = statement.executeUpdate(sqlQuery)
    val ans: String = s"insert $result row values $info"
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
