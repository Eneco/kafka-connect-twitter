package com.eneco.trading.kafka.connect.twitter

import scala.collection.JavaConverters._

class TestTwitterSinkConfig extends TestTwitterBase {
    test("A TestTwitterSinkConfig should be correctly configured") {
      val config = getSinkConfig
      val taskConfig = new TwitterSinkConfig(config.asJava)
      taskConfig.getString(TwitterSinkConfig.CONSUMER_KEY_CONFIG) shouldBe "test"
      taskConfig.getString(TwitterSinkConfig.CONSUMER_SECRET_CONFIG) shouldBe "c-secret"
      taskConfig.getString(TwitterSinkConfig.SECRET_CONFIG) shouldBe "secret"
      taskConfig.getString(TwitterSinkConfig.TOKEN_CONFIG) shouldBe "token"
      taskConfig.getList(TwitterSinkConfig.TOPICS) shouldBe Seq("just-a-sink-topic").asJava
    }
  }
