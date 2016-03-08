package com.eneco.trading.kafka.connect.twitter

import java.util.concurrent.LinkedBlockingQueue

import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.Constants
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import com.twitter.hbc.httpclient.auth.OAuth1
import org.apache.kafka.connect.source.SourceTaskContext

/**
  * Created by andrew@datamountaineer.com on 24/02/16. 
  * kafka-connect-twitter
  */
object TwitterReader {
  def apply(config: TwitterSourceConfig, context: SourceTaskContext) = {
    //endpoints
    val endpoint = new StatusesFilterEndpoint()
    endpoint.stallWarnings(false)
    endpoint.trackTerms(config.getList(TwitterSourceConfig.TRACK_TERMS))

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

    //build basic client
    val client = new ClientBuilder()
      .name(config.getString(TwitterSourceConfig.TWITTER_APP_NAME))
      .hosts(Constants.STREAM_HOST)
      .endpoint(endpoint)
      .authentication(auth)
      .processor(new StringDelimitedProcessor(queue))
      .build()

    new TwitterStatusReader(client = client, rawQueue = queue, batchSize = batchSize, batchTimeout = batchTimeout, topic = topic)
  }
}
