package org.customAsync

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}
import org.data.{MysqlSyncClient, CustomUser}

import java.util.concurrent.{LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}


class MysqlAsyncRichMapFunction extends RichAsyncFunction[CustomUser, String] {
  private val DEFAULT_CLIENT_THREAD_NUM: Int = 8
  var client: MysqlSyncClient[CustomUser] = _
  var executorService: ThreadPoolExecutor = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    client = new MysqlSyncClient[CustomUser]
    executorService = new ThreadPoolExecutor(
      DEFAULT_CLIENT_THREAD_NUM,
      DEFAULT_CLIENT_THREAD_NUM+2,
      0L,
      TimeUnit.MILLISECONDS,
      new LinkedBlockingQueue())
  }

  override def close(): Unit = super.close()

  override def asyncInvoke(input: CustomUser, resultFuture: ResultFuture[String]): Unit = {
    executorService.execute(new Runnable() {
      override def run(): Unit = {
        resultFuture.complete(Iterable(client.query(input)))
      }
    })
  }
}