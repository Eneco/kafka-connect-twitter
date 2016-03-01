package com.eneco.trading.kafka.connect.twitter

import org.scalatest.{FunSuite, Matchers, BeforeAndAfter}

/**
  * Created by andrew@datamountaineer.com on 29/02/16. 
  * kafka-connect-twitter
  */
trait TestTwitterBase extends FunSuite with Matchers with BeforeAndAfter {
  def getConfig = {
    Map(TwitterSourceConfig.CONSUMER_KEY_CONFIG->"test",
      TwitterSourceConfig.CONSUMER_SECRET_CONFIG->"c-secret",
      TwitterSourceConfig.SECRET_CONFIG->"secret",
      TwitterSourceConfig.TOKEN_CONFIG->"token",
      TwitterSourceConfig.TRACK_TERMS->"term1",
      TwitterSourceConfig.TWITTER_APP_NAME->"myApp"
    )
  }
}
