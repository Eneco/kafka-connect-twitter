package com.eneco.trading.kafka.connect.twitter

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import java.util
import com.google.common.collect.Queues

/**
  * Created by r on 3/1/16.
  */
object Extensions {

  implicit class LinkedBlockingQueueExtension[T](val lbq: LinkedBlockingQueue[T]) extends AnyVal {
    def drainWithTimeoutTo(collection: util.Collection[_ >: T], maxElements: Int, timeout: Long, unit: TimeUnit): Int = {
      Queues.drain[T](lbq, collection, maxElements, timeout, unit)
    }
  }
}
