package com.eneco.trading.kafka.connect.twitter

import java.util
import java.util.concurrent.{TimeUnit, LinkedBlockingQueue, Executors}
import com.eneco.trading.kafka.connect.twitter.domain.TwitterStatus
import com.twitter.hbc.httpclient.BasicClient
import com.twitter.hbc.twitter4j.Twitter4jStatusClient
import org.apache.kafka.connect.source.SourceRecord
import twitter4j._
import scala.collection.JavaConverters._
import Extensions._

class StatusEnqueuer(queue: LinkedBlockingQueue[Status]) extends StatusListener with Logging {
  override def onStallWarning(stallWarning: StallWarning) = log.warn("onStallWarning")
  override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice) = log.info("onDeletionNotice")

  override def onScrubGeo(l: Long, l1: Long) = {
    log.info(s"onScrubGeo $l $l1")
  }

  override def onStatus(status: Status) = {
    log.info("onStatus")
    queue.put(status)
  }

  override def onTrackLimitationNotice(i: Int) = log.info(s"onTrackLimitationNotice $i")
  override def onException(e: Exception)= log.warn("onException " + e.toString)
}

/**
  * Created by andrew@datamountaineer.com on 24/02/16. 
  * kafka-connect-twitter
  */
class TwitterStatusReader(client: BasicClient, rawQueue: LinkedBlockingQueue[String], batchSize : Int, topic: String) extends Logging {
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
    statusQueue.drainWithTimeoutTo(l, batchSize, 1, TimeUnit.SECONDS)
    l.asScala.map(buildRecords).asJava
  }

  /**
    * Build a List of SourceRecords
    *
    * @param status A Twitter4j Status
    * @return A list of SourceRecords
    * */
  def buildRecords(status: Status) : SourceRecord = {
    val ts = TwitterStatus.struct(TwitterStatus(status))
    new SourceRecord(
        Map("tweetSource"-> status.getSource).asJava, //source partitions?
        Map("tweetId"-> status.getId).asJava, //source offsets?
        topic,
        ts.schema(),
        ts)
  }

  /**
    * Stop the HBC client
    * */
  def stop() = {
    log.info("Stop Twitter client")
    client.stop()
  }
}
