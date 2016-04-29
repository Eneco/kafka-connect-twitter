package com.eneco.trading.kafka.connect.twitter

import java.util
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}
import scala.collection.JavaConverters._
import scala.util.{Success, Failure}

class TwitterSinkTask extends SinkTask with Logging {
  var writer: Option[SimpleTwitterWriter] = None

  override def start(props: util.Map[String, String]): Unit = {
    val sinkConfig = new TwitterSinkConfig(props)
    writer = Some(new TwitterWriter(
      sinkConfig.getString(TwitterSinkConfig.CONSUMER_KEY_CONFIG),
      sinkConfig.getPassword(TwitterSinkConfig.CONSUMER_SECRET_CONFIG).value,
      sinkConfig.getString(TwitterSinkConfig.TOKEN_CONFIG),
      sinkConfig.getPassword(TwitterSinkConfig.SECRET_CONFIG).value))
  }

  override def put(records: util.Collection[SinkRecord]): Unit =
    records.asScala
      .map(_.value.toString)
      .map(text => (text, writer match {
        case Some(writer) => writer.updateStatus(text)
        case None => Failure(new IllegalStateException("twitter writer is not set"))
      }))
      .foreach {
        case (text, result) => result match {
          case Success(id) => log.info(s"successfully tweeted `${text}`; got assigned id ${id}")
          case Failure(err) => log.warn(s"tweeting `${text}` failed: ${err.getMessage}")
        }
      }

  override def stop(): Unit = {
  }

  override def flush(map: util.Map[TopicPartition, OffsetAndMetadata]) = {
  }
  override def version(): String = ""
}