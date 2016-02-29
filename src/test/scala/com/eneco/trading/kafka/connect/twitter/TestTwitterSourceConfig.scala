package com.eneco.trading.kafka.connect.twitter


import scala.collection.JavaConverters._
/**
  * Created by andrew@datamountaineer.com on 29/02/16. 
  * kafka-connect-twitter
  */
class TestTwitterSourceConfig extends TestTwitterBase {
  test("A TwitterSourceConfig should be correctly configured") {
    val config = getConfig
    val taskConfig = new TwitterSourceConfig(config.asJava)
    taskConfig.getString(TwitterSourceConfig.CONSUMER_KEY_CONFIG) shouldBe "test"
    taskConfig.getString(TwitterSourceConfig.CONSUMER_SECRET_CONFIG) shouldBe "secret"
    taskConfig.getString(TwitterSourceConfig.SECRET_CONFIG) shouldBe "secret"
    taskConfig.getString(TwitterSourceConfig.TOKEN_CONFIG) shouldBe "token"
    taskConfig.getList(TwitterSourceConfig.TRACK_TERMS).asScala.head shouldBe "term1"
    taskConfig.getString(TwitterSourceConfig.TWITTER_APP_NAME) shouldBe "myApp"
  }
}
