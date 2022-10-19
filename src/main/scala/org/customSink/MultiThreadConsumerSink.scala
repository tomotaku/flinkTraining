package org.customSink

import org.apache.flink.calcite.shaded.com.google.common.collect.Queues
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.data.CustomUser
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.{CyclicBarrier, LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}

class MultiThreadConsumerSink extends RichSinkFunction[CustomUser] {
  private val DEFAULT_CLIENT_THREAD_NUM: Int = 10
  private val DEFAULT_QUEUE_CAPACITY: Int = 5000
  private val LOG: Logger = LoggerFactory.getLogger(classOf[MultiThreadConsumerSink])
  var bufferQueue: LinkedBlockingQueue[CustomUser] = _
  var clientBarrier: CyclicBarrier = _

  def this(bufferQueue: LinkedBlockingQueue[CustomUser], clientBarrier: CyclicBarrier) {
    this()
    this.bufferQueue = bufferQueue
    this.clientBarrier = clientBarrier
  }

  override def open(parameters: Configuration): Unit = {

    super.open(parameters)
    // new 一个容量为 DEFAULT_CLIENT_THREAD_NUM 的线程池
    val threadPoolExecutor: ThreadPoolExecutor = new ThreadPoolExecutor(DEFAULT_CLIENT_THREAD_NUM, DEFAULT_CLIENT_THREAD_NUM,
      0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue())
    // new 一个容量为 DEFAULT_QUEUE_CAPACITY 的数据缓冲队列
    bufferQueue = Queues.newLinkedBlockingQueue(DEFAULT_QUEUE_CAPACITY)
    // barrier 需要拦截 (DEFAULT_CLIENT_THREAD_NUM + 1) 个线程
    this.clientBarrier = new CyclicBarrier(DEFAULT_CLIENT_THREAD_NUM + 1)
    val consumerClient: MultiThreadConsumerClient = new MultiThreadConsumerClient(bufferQueue, clientBarrier)
    for (i <- 0 until DEFAULT_CLIENT_THREAD_NUM) {
      threadPoolExecutor.execute(consumerClient)
      println(s"create thread $i")
    }
  }

  override def invoke(value: CustomUser, context: SinkFunction.Context): Unit = {
    LOG.info("sinkto: " + value.toString)
    LOG.info("sinktobufferQueue: " + bufferQueue.hashCode())
    bufferQueue.put(value)
    LOG.info("bufferQueue size: " + bufferQueue.size())
    //    LOG.info("contains: "+bufferQueue.toString)
  }
   def snapshotState(functionSnapshotContext:FunctionSnapshotContext): Unit ={
    LOG.info("snapshotState : 所有的 client 准备 flush !!!")
    // barrier 开始等待
    clientBarrier.await()
  }
   def initializeState(functionInitializationContext:FunctionInitializationContext):Unit={
  }
}