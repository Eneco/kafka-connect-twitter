package com.eneco.trading.kafka.connect.twitter

import java.util.concurrent.LinkedBlockingQueue

import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.Constants
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint
import com.twitter.hbc.core.endpoint.DefaultStreamingEndpoint
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import com.twitter.hbc.core.endpoint.Location
import com.twitter.hbc.httpclient.auth.OAuth1
import org.apache.kafka.connect.source.{SourceRecord, SourceTaskContext}
import twitter4j.Status
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 24/02/16. 
  * kafka-connect-twitter
  */
object TwitterReader {
  def apply(config: TwitterSourceConfig, context: SourceTaskContext) = {
    //endpoints
    val endpoint: DefaultStreamingEndpoint = if (config.getString(TwitterSourceConfig.STREAM_TYPE).equals(TwitterSourceConfig.STREAM_TYPE_SAMPLE)) {
      new StatusesSampleEndpoint()
    } else {
      val trackEndpoint = new StatusesFilterEndpoint()
      val terms = config.getList(TwitterSourceConfig.TRACK_TERMS) 
      if (!terms.isEmpty) {
        trackEndpoint.trackTerms(terms)
      }
      val locs = config.getList(TwitterSourceConfig.TRACK_LOCATIONS)
      if (!locs.isEmpty) {
        val locations = locs.toList.map({ x => Double.box(x.toDouble)}).grouped(4).toList
            .map({ l => new Location(new Location.Coordinate(l(0), l(1)), new Location.Coordinate(l(2), l(3)))})
            .asJava
        trackEndpoint.locations(locations)
      }
      val follow = config.getList(TwitterSourceConfig.TRACK_FOLLOW) 
      if (!follow.isEmpty) {
        val users = follow.toList.map({ x => Long.box(x.trim.toLong)}).asJava
        trackEndpoint.followings(users)
      }
      trackEndpoint
    }
    endpoint.stallWarnings(false)
    val language = config.getList(TwitterSourceConfig.LANGUAGE) 
    if (!language.isEmpty) {
      // endpoint.languages(language) doesn't work as intended!
      endpoint.addQueryParameter(TwitterSourceConfig.LANGUAGE, language.toList.mkString(","))
    }

    //twitter auth stuff
    val auth = new OAuth1(config.getString(TwitterSourceConfig.CONSUMER_KEY_CONFIG),
      config.getString(TwitterSourceConfig.CONSUMER_SECRET_CONFIG),
      config.getString(TwitterSourceConfig.TOKEN_CONFIG),
      config.getString(TwitterSourceConfig.SECRET_CONFIG))

    //batch size to take from the queue
    val batchSize = config.getInt(TwitterSourceConfig.BATCH_SIZE)
    val batchTimeout = config.getDouble(TwitterSourceConfig.BATCH_TIMEOUT)

    //The Kafka topic to append to
    val topic = config.getString(TwitterSourceConfig.TOPIC)

    //queue for client to buffer to
    val queue = new LinkedBlockingQueue[String](10000)

    //how the output is formatted
    val statusConverter = config.getString(TwitterSourceConfig.OUTPUT_FORMAT) match {
      case TwitterSourceConfig.OUTPUT_FORMAT_ENUM_STRING => StatusToStringKeyValue
      case TwitterSourceConfig.OUTPUT_FORMAT_ENUM_STRUCTURED => StatusToTwitterStatusStructure
    }

    //build basic client
    val client = new ClientBuilder()
      .name(config.getString(TwitterSourceConfig.TWITTER_APP_NAME))
      .hosts(Constants.STREAM_HOST)
      .endpoint(endpoint)
      .authentication(auth)
      .processor(new StringDelimitedProcessor(queue))
      .build()

    new TwitterStatusReader(client = client, rawQueue = queue, batchSize = batchSize, 
        batchTimeout = batchTimeout, topic = topic, statusConverter = statusConverter)
  }
}
