package com.eneco.trading.kafka.connect.twitter

import org.apache.kafka.connect.sink.SinkRecord
import scala.collection.JavaConverters._
import scala.util.{Success, Try}

class TestSinkTask extends TestTwitterBase {
  test("Strings put to to Task are tweeted") {
    val sinkTask = new TwitterSinkTask()
    val myTestTweet = "I tweet, ergo sum."
    sinkTask.writer = Some(new SimpleTwitterWriter {
      //TODO: use DI?
      def updateStatus(s: String): Try[Long] = {
        s shouldEqual myTestTweet
        Success(5)
      }
    })
    val sr = new SinkRecord("topic", 5, null, null, null, myTestTweet, 123)
    sinkTask.put(Seq(sr).asJava)
  }

}
