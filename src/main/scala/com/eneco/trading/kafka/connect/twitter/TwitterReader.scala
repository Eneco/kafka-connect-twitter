package com.eneco.trading.kafka.connect.twitter

import java.util.concurrent.LinkedBlockingQueue

import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.Constants
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import com.twitter.hbc.httpclient.auth.OAuth1

/**
  * Created by andrew@datamountaineer.com on 24/02/16. 
  * kafka-connect-twitter
  */
object TwitterReader {
  def apply(config: TwitterSourceConfig) = {
    val endpoint = new StatusesFilterEndpoint()
    endpoint.stallWarnings(false)
    endpoint.trackTerms(config.getList(TwitterSourceConfig.TRACK_TERMS))

    val auth = new OAuth1(config.getString(TwitterSourceConfig.CONSUMER_KEY_CONFIG),
      config.getString(TwitterSourceConfig.CONSUMER_SECRET_CONFIG),
      config.getString(TwitterSourceConfig.TOKEN_CONFIG),
      config.getString(TwitterSourceConfig.SECRET_CONFIG))

    val queue= new LinkedBlockingQueue[String](10000)

    val client = new ClientBuilder()
                    .name(config.getString(TwitterSourceConfig.TWITTER_APP_NAME))
                    .hosts(Constants.STREAM_HOST)
                    .endpoint(endpoint)
                    .authentication(auth)
                    .processor(new StringDelimitedProcessor(queue))
                    .build()

    new TwitterStreamReader(client = client)
  }
}
