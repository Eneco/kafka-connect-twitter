package com.eneco.trading.kafka.connect.twitter

import scala.collection.JavaConverters._

class TestTwitterSinkConfig extends TestTwitterBase {
    test("A TestTwitterSinkConfig should be correctly configured") {
      val config = getSinkConfig
      val taskConfig = new TwitterSinkConfig(config.asJava)
      taskConfig.getString(TwitterSinkConfig.CONSUMER_KEY_CONFIG) shouldBe "test"
      taskConfig.getPassword(TwitterSinkConfig.CONSUMER_SECRET_CONFIG).value shouldBe "c-secret"
      taskConfig.getPassword(TwitterSinkConfig.SECRET_CONFIG).value shouldBe "secret"
      taskConfig.getString(TwitterSinkConfig.TOKEN_CONFIG) shouldBe "token"
      taskConfig.getList(TwitterSinkConfig.TOPICS) shouldBe Seq("just-a-sink-topic").asJava
    }
  }
