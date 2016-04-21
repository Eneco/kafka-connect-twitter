package com.eneco.trading.kafka.connect.twitter

import twitter4j.auth.AccessToken
import twitter4j.{TwitterFactory, Twitter}

import scala.util.Try

trait SimpleTwitterWriter {
  def updateStatus(s: String): Try[Long]
}

class TwitterWriter(consumer: String, consumerSecret: String, access: String, accessSecret: String, twitterClient: Twitter = new TwitterFactory().getInstance()) extends SimpleTwitterWriter {
  twitterClient.setOAuthConsumer(consumer, consumerSecret)
  twitterClient.setOAuthAccessToken(new AccessToken(access, accessSecret))

  def updateStatus(s: String): Try[Long] = {
    Try(twitterClient.updateStatus(s).getId)
  }
}
