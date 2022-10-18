package org.customAsync

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}
import org.data.{MysqlSyncClient, user}

import java.util.Collections
import java.util.concurrent.{Future, LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}


class MysqlAsyncRichMapFunction extends RichAsyncFunction[user, Stream[String]] {
  private val DEFAULT_CLIENT_THREAD_NUM: Int = 10
  var client: MysqlSyncClient[user] = _
  var executorService: ThreadPoolExecutor = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    client = new MysqlSyncClient[user]
    executorService = new ThreadPoolExecutor(
      DEFAULT_CLIENT_THREAD_NUM,
      DEFAULT_CLIENT_THREAD_NUM,
      0L,
      TimeUnit.MILLISECONDS,
      new LinkedBlockingQueue())
  }

  override def close(): Unit = super.close()

  override def asyncInvoke(input: user , resultFuture: ResultFuture[user]): Unit = {
//    executorService.execute(new Runnable() {
//
//      override def run(): Unit = {
//        resultFuture.complete(
//          Collections.singletonList[Stream[String]](
//            client.query(input)
//          )
//        )
//      }
//    }

    val resultFutureRequested: Future[String] = client.query(input)
    resultFutureRequested.onSuccess {
      case result: String => resultFuture.complete(Iterable((str, result)))
    }
  }

  override def timeout(input: user, resultFuture: ResultFuture[user]): Unit = {
    println("async call time out!")
    //    input.setParentCategoryId(Long.MIN_VALUE)
    resultFuture.complete(Collections.singleton(input))
  }
}