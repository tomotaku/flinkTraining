package org.data
case class AccountInfo() extends Serializable {
   val JDBC_DRIVER: String = "com.mysql.cj.jdbc.Driver"
   val sourceJdbcUrl: String = "jdbc:mysql://localhost:3306/test"
   val sourceJdbcUserName: String = "root"
   val sourceJdbcPassword: String = "123456"
   val sinkJdbcUrl: String = "jdbc:mysql://localhost:3306/test?allowPublicKeyRetrieval=true&useSSL=false"
   val sinkJdbcUserName: String = "root"
   val sinkJdbcPassword: String = "123456"
//   val sourceJdbcUrl: String = "jdbc:mysql://localhost:3306/test"
//   val sourceJdbcUserName: String = "root"
//   val sourceJdbcPassword: String = "SJjz@2020"
//   val sinkJdbcUrl: String = "jdbc:mysql://localhost:3306/test?allowPublicKeyRetrieval=true&useSSL=false"
//   val sinkJdbcUserName: String = "root"
//   val sinkJdbcPassword: String = "SJjz@2020"

}
