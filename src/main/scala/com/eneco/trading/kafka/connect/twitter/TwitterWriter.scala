package com.eneco.trading.kafka.connect.twitter

import twitter4j.auth.AccessToken
import twitter4j.{TwitterFactory, Twitter}

import scala.util.Try

trait SimpleTwitterWriter {
  /**
    * Performs a status update
    * @param s the status
    * @return A Try id of the new status as assigned by the Twitter api
    */
  def updateStatus(s: String): Try[Long]
}

/**
  * Allows one to update statuses
  * @param consumer apps.twitter.com | Keys and Access Tokens: 	Consumer Key (API Key)
  * @param consumerSecret apps.twitter.com | Keys and Access Tokens: Consumer Secret (API Secret)
  * @param access apps.twitter.com | Keys and Access Tokens: Access Token
  * @param accessSecret apps.twitter.com | Keys and Access Tokens: Access Token Secret
  * @param twitterClient poor man's DI: something that implements the Twitter4j Twitter interface
  */
class TwitterWriter(consumer: String, consumerSecret: String, access: String, accessSecret: String, twitterClient: Twitter = new TwitterFactory().getInstance()) extends SimpleTwitterWriter {
  twitterClient.setOAuthConsumer(consumer, consumerSecret)
  twitterClient.setOAuthAccessToken(new AccessToken(access, accessSecret))

  /**
    * Performs a status update
    * @param s the status
    * @return A Try id of the new status as assigned by the Twitter api
    */
  def updateStatus(s: String): Try[Long] = {
    Try(twitterClient.updateStatus(s).getId)
  }
}
