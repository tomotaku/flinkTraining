package org.data

import java.sql._

class MysqlSyncClient[user] {
  val JDBC_DRIVER: String = "com.mysql.jdbc.Driver"
  val URL: String = "jdbc:mysql://localhost:3306/flink"
  val USER: String = "root"
  val PASSWORD: String = "123456"
  var connection: Connection = _

  try {
    Class.forName(JDBC_DRIVER)
  } catch {
    case e: ClassNotFoundException => println("Driver not found!" + e.getMessage)
  }
  try {
    connection = DriverManager.getConnection(URL, USER, PASSWORD)
  } catch {
    case e: SQLException => println("init connection failed!" + e.getMessage)
  }

  def query(t: user): Stream[String] = {
    val info = t.toString.split(",")
    val sqlQuery: String = s"insert table xxx values ($info(1),$info(2),$info(3),$info(4),$info(5))"
    val statement: Statement = connection.createStatement()
    val resultSet: ResultSet = statement.executeQuery(sqlQuery)
    val result = new Iterator[String] {
      def hasNext = resultSet.next()
      def next() = resultSet.getString(1)
    }
    result.toStream
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
