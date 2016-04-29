package com.eneco.trading.kafka.connect.twitter

import java.util

import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.connect.sink.SinkTask

object TwitterSinkConfig {
  val CONSUMER_KEY_CONFIG = "twitter.consumerkey"
  val CONSUMER_KEY_CONFIG_DOC = "Twitter account consumer key."
  val CONSUMER_SECRET_CONFIG = "twitter.consumersecret"
  val CONSUMER_SECRET_CONFIG_DOC = "Twitter account consumer secret."
  val TOKEN_CONFIG = "twitter.token"
  val TOKEN_CONFIG_DOC = "Twitter account token."
  val SECRET_CONFIG = "twitter.secret"
  val SECRET_CONFIG_DOC = "Twitter account secret."
  val TOPICS = SinkTask.TOPICS_CONFIG
  val TOPICS_DOC = "The Kafka topic to read from."

  val config: ConfigDef = new ConfigDef()
    .define(CONSUMER_KEY_CONFIG, Type.STRING, Importance.HIGH, CONSUMER_KEY_CONFIG_DOC)
    .define(CONSUMER_SECRET_CONFIG, Type.PASSWORD, Importance.HIGH, CONSUMER_SECRET_CONFIG_DOC)
    .define(TOKEN_CONFIG, Type.STRING, Importance.HIGH, TOKEN_CONFIG_DOC)
    .define(SECRET_CONFIG, Type.PASSWORD, Importance.HIGH, SECRET_CONFIG_DOC)
    .define(TOPICS, Type.LIST, Importance.HIGH, TOPICS_DOC)
}

class TwitterSinkConfig(props: util.Map[String, String])
  extends AbstractConfig(TwitterSinkConfig.config, props) {
}
