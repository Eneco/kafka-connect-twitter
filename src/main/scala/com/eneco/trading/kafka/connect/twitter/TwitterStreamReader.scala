package com.eneco.trading.kafka.connect.twitter

import java.util.concurrent.{Executors, TimeUnit, LinkedBlockingQueue}

import com.twitter.hbc.httpclient.BasicClient
import com.twitter.hbc.twitter4j.Twitter4jStatusClient
import org.apache.kafka.connect.source.SourceRecord
import twitter4j.{StallWarning, Status, StatusDeletionNotice, StatusListener}
import scala.collection.JavaConverters._

class StatusEnqueuer(queue: LinkedBlockingQueue[Status]) extends StatusListener with Logging {
  override def onStallWarning(stallWarning: StallWarning): Unit = {
    log.warn("onStallWarning")
  }

  override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice): Unit = {
    log.info("onDeletionNotice")
  }

  override def onScrubGeo(l: Long, l1: Long): Unit = {
    log.info(s"onScrubGeo $l $l1")
  }

  override def onStatus(status: Status): Unit = {
    log.info("onStatus")
    queue.put(status)
  }

  override def onTrackLimitationNotice(i: Int): Unit = {
    log.info(s"onTrackLimitationNotice $i")
  }

  override def onException(e: Exception): Unit = {
    log.warn("onException " + e.toString)
  }
}

/**
  * Created by andrew@datamountaineer.com on 24/02/16. 
  * kafka-connect-twitter
  */
class TwitterStreamReader(client: BasicClient, rawqueue: LinkedBlockingQueue[String]) extends Logging {
  log.info("Initialising Twitter Stream Reader")
  val statusqueue = new LinkedBlockingQueue[Status](10000)

  val t4jclient = new Twitter4jStatusClient(client, rawqueue, List[StatusListener](new StatusEnqueuer(statusqueue)).asJava, Executors.newFixedThreadPool(1) )
  t4jclient.connect()
  t4jclient.process()

  def poll() : List[SourceRecord] = {
    log.info("poll")
    if (client.isDone) {
      log.warn("Client connection closed unexpectedly: ", client.getExitEvent.getMessage)
      return null;
    }
    Option(statusqueue.poll(1, TimeUnit.SECONDS)) match {
      case Some(msg) => {
        log.info(msg.getText)
        return List[SourceRecord]()
      }
      case _ => return List[SourceRecord]()
    }
    List[SourceRecord]()
  }

  /**
    * Stop the HBC client
    * */
  def stop() = {
    client.stop()
  }
}
