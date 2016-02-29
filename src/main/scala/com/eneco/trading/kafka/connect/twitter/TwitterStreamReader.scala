package com.eneco.trading.kafka.connect.twitter

import java.util.Collections
import java.util.concurrent.{Executors, LinkedBlockingQueue, TimeUnit}
import com.twitter.hbc.httpclient.BasicClient
import com.twitter.hbc.twitter4j.Twitter4jStatusClient
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.source.SourceRecord
import twitter4j._
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

case class TwitterStatus(id: Long, createdAt: String, favoriteCount: Int, text: String, user: TwitterUser)
case class TwitterUser(id: Long, name: String, screenName: String)

object TwitterUser {
  def apply(u: User) = {
    new TwitterUser(id = u.getId, name = u.getName, screenName = u.getScreenName)
  }

  def struct(u: TwitterUser) =
    new Struct(schema)
      .put("id", u.id)
      .put("name", u.name)
      .put("screenName", u.screenName)

  val schema = SchemaBuilder.struct().name("TwitterUser")
    .field("id", Schema.INT64_SCHEMA)
    .field("name", Schema.STRING_SCHEMA)
    .field("screenName", Schema.STRING_SCHEMA)
    .build()
}

object TwitterStatus {
  def apply(s: Status) = {
    new TwitterStatus(id = s.getId, createdAt = s.getCreatedAt.toString, favoriteCount = s.getFavoriteCount, text = s.getText, user = TwitterUser(s.getUser))
  }

  def struct(s: TwitterStatus) =
    new Struct(schema)
      .put("id", s.id)
      .put("createdAt", s.createdAt)
      .put("favoriteCount", s.favoriteCount)
      .put("text", s.text)
      .put("user", TwitterUser.struct(s.user))

  val schema = SchemaBuilder.struct().name("TwitterStatus")
    .field("id", Schema.INT64_SCHEMA)
    .field("createdAt", Schema.STRING_SCHEMA)
    .field("favoriteCount", Schema.INT32_SCHEMA)
    .field("text", Schema.STRING_SCHEMA)
    .field("user", TwitterUser.schema)
    .build()
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
    return Option(statusqueue.poll(1, TimeUnit.SECONDS)) match {
      case Some(status) => {
        val ts = TwitterStatus.struct(TwitterStatus(status))
        List[SourceRecord](new SourceRecord(Collections.singletonMap("TODO", "TODO"), Collections.singletonMap("TODO2", "TODO2"), "tweeters", ts.schema(), ts))
      }
      case _ => List[SourceRecord]()
    }
  }

  /**
    * Stop the HBC client
    * */
  def stop() = {
    client.stop()
  }
}
