package com.eneco.trading.kafka.connect.twitter

import java.util
import java.util.concurrent.{TimeUnit, LinkedBlockingQueue, Executors}
import com.eneco.trading.kafka.connect.twitter.domain.TwitterStatus
import com.twitter.hbc.httpclient.BasicClient
import com.twitter.hbc.twitter4j.Twitter4jStatusClient
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.source.SourceRecord
import twitter4j._
import scala.collection.JavaConverters._
import Extensions._

class StatusEnqueuer(queue: LinkedBlockingQueue[Status]) extends StatusListener with Logging {
  override def onStallWarning(stallWarning: StallWarning) = log.warn("onStallWarning")
  override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice) = log.info("onDeletionNotice")

  override def onScrubGeo(l: Long, l1: Long) = {
    log.debug(s"onScrubGeo $l $l1")
  }

  override def onStatus(status: Status) = {
    log.debug("onStatus")
    queue.put(status)
  }

  override def onTrackLimitationNotice(i: Int) = log.info(s"onTrackLimitationNotice $i")
  override def onException(e: Exception)= log.warn("onException " + e.toString)
}

trait StatusToSourceRecord {
  def convert(status: Status, topic: String): SourceRecord
}

object StatusToStringKeyValue extends StatusToSourceRecord {
  def convert (status: Status, topic: String): SourceRecord = {
    new SourceRecord(
      Map("tweetSource" -> status.getSource).asJava, //source partitions?
      Map("tweetId" -> status.getId).asJava, //source offsets?
      topic,
      null,
      Schema.STRING_SCHEMA,
      status.getUser.getScreenName,
      Schema.STRING_SCHEMA,
      status.getText,
      status.getCreatedAt.getTime)
  }
}

object StatusToTwitterStatusStructure extends StatusToSourceRecord {
  def convert(status: Status, topic: String): SourceRecord = {
    //val ts = TwitterStatus.struct(TwitterStatus(status))
    new SourceRecord(
      Map("tweetSource" -> status.getSource).asJava, //source partitions?
      Map("tweetId" -> status.getId).asJava, //source offsets?
      topic,
      null,
      Schema.STRING_SCHEMA,
      status.getUser.getScreenName,
      TwitterStatus.schema,
      TwitterStatus.struct(status),
      status.getCreatedAt.getTime)
  }
}

/**
  * Created by andrew@datamountaineer.com on 24/02/16.
  * kafka-connect-twitter
  */
class TwitterStatusReader(client: BasicClient,
                          rawQueue: LinkedBlockingQueue[String],
                          batchSize : Int,
                          batchTimeout: Double,
                          topic: String,
                          statusConverter: StatusToSourceRecord = StatusToTwitterStatusStructure
                         ) extends Logging {
  log.info("Initialising Twitter Stream Reader")
  val statusQueue = new LinkedBlockingQueue[Status](10000)

  //Construct the status client
  val t4jClient = new Twitter4jStatusClient(
                        client,
                        rawQueue,
                        List[StatusListener](new StatusEnqueuer(statusQueue)).asJava,
                        Executors.newFixedThreadPool(1))

  //connect and subscribe
  t4jClient.connect()
  t4jClient.process()

  /**
    * Drain the queue
    *
    * @return A List of SourceRecords
    * */
  def poll() : util.List[SourceRecord] = {
    if (client.isDone) log.warn("Client connection closed unexpectedly: ", client.getExitEvent.getMessage) //TODO: what next?

    val l = new util.ArrayList[Status]()
    statusQueue.drainWithTimeoutTo(l, batchSize, (batchTimeout * 1E9).toLong, TimeUnit.NANOSECONDS)
    l.asScala.map(statusConverter.convert(_, topic)).asJava
  }

  /**
    * Stop the HBC client
    * */
  def stop() = {
    log.info("Stop Twitter client")
    client.stop()
  }


}
