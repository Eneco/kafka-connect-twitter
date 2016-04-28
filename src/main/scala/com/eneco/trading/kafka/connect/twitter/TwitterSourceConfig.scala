package com.eneco.trading.kafka.connect.twitter

import java.util

import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}
import org.apache.kafka.common.config.ConfigDef.{Type, Importance}
import scala.collection.JavaConversions._
import scala.util.{Failure, Try}

/**
  * Created by andrew@datamountaineer.com on 24/02/16.
  * kafka-connect-twitter
  */
object TwitterSourceConfig {
  val CONSUMER_KEY_CONFIG = "twitter.consumerkey"
  val CONSUMER_KEY_CONFIG_DOC = "Twitter account consumer key."
  val CONSUMER_SECRET_CONFIG = "twitter.consumersecret"
  val CONSUMER_SECRET_CONFIG_DOC = "Twitter account consumer secret."
  val TOKEN_CONFIG = "twitter.token"
  val TOKEN_CONFIG_DOC = "Twitter account token."
  val SECRET_CONFIG = "twitter.secret"
  val SECRET_CONFIG_DOC = "Twitter account secret."
  val STREAM_TYPE = "stream.type"
  val STREAM_TYPE_DOC = "Twitter stream type (filter or sample)."
  val STREAM_TYPE_FILTER = "filter"
  val STREAM_TYPE_SAMPLE = "sample"
  val STREAM_TYPE_DEFAULT = STREAM_TYPE_FILTER
  val TRACK_TERMS = "track.terms"
  val TRACK_TERMS_DOC = "Twitter terms to track."
  val TRACK_LOCATIONS = "track.locations"
  val TRACK_LOCATIONS_DOC = "Geo locations to track."
  val TRACK_FOLLOW = "track.follow"
  val TRACK_FOLLOW_DOC = "User IDs to track."
  val TWITTER_APP_NAME = "twitter.app.name"
  val TWITTER_APP_NAME_DOC = "Twitter app name"
  val TWITTER_APP_NAME_DEFAULT = "KafkaConnectTwitterSource"
  val BATCH_SIZE = "batch.size"
  val BATCH_SIZE_DOC = "Batch size to write to Kafka (Drains the queue supplied to the hbc client)."
  val BATCH_SIZE_DEFAULT = 100
  val BATCH_TIMEOUT = "batch.timeout"
  val BATCH_TIMEOUT_DOC = "Batch timeout in seconds to write to Kafka (Drains the queue supplied to the hbc client)."
  val BATCH_TIMEOUT_DEFAULT = 0.1
  val TOPIC = "topic"
  val TOPIC_DOC = "The Kafka topic to append to"
  val TOPIC_DEFAULT = "tweets"
  val LANGUAGE = "language"
  val LANGUAGE_DOC = "List of languages to filter"
  val OUTPUT_FORMAT = "output.format"
  val OUTPUT_FORMAT_ENUM_STRUCTURED = "structured"
  val OUTPUT_FORMAT_ENUM_STRING = "string"
  val OUTPUT_FORMAT_DOC = s"How the output is formatted, can be either ${OUTPUT_FORMAT_ENUM_STRING} for (key=username:string, value=text:string), or ${OUTPUT_FORMAT_ENUM_STRUCTURED} for value=structure:TwitterStatus."
  val OUTPUT_FORMAT_DEFAULT = "structured"
  val EMPTY_VALUE = ""

  val config: ConfigDef = new ConfigDef()
    .define(CONSUMER_KEY_CONFIG, Type.STRING, Importance.HIGH, CONSUMER_KEY_CONFIG_DOC)
    .define(CONSUMER_SECRET_CONFIG, Type.STRING, Importance.HIGH, CONSUMER_SECRET_CONFIG_DOC)
    .define(TOKEN_CONFIG, Type.STRING, Importance.HIGH, TOKEN_CONFIG_DOC)
    .define(SECRET_CONFIG, Type.STRING, Importance.HIGH, SECRET_CONFIG_DOC)
    .define(STREAM_TYPE, Type.STRING, STREAM_TYPE_DEFAULT, Importance.HIGH, STREAM_TYPE_DOC)
    .define(TRACK_TERMS, Type.LIST, EMPTY_VALUE, Importance.MEDIUM, TRACK_TERMS_DOC)
    .define(TRACK_FOLLOW, Type.LIST, EMPTY_VALUE, Importance.MEDIUM, TRACK_FOLLOW_DOC)
    .define(TRACK_LOCATIONS, Type.LIST, EMPTY_VALUE, Importance.MEDIUM, TRACK_LOCATIONS_DOC)
    .define(TWITTER_APP_NAME, Type.STRING, TWITTER_APP_NAME_DEFAULT, Importance.HIGH, TWITTER_APP_NAME_DOC)
    .define(BATCH_SIZE, Type.INT, BATCH_SIZE_DEFAULT, Importance.MEDIUM, BATCH_SIZE_DOC)
    .define(BATCH_TIMEOUT, Type.DOUBLE, BATCH_TIMEOUT_DEFAULT, Importance.MEDIUM, BATCH_TIMEOUT_DOC)
    .define(TOPIC, Type.STRING, TOPIC_DEFAULT, Importance.HIGH, TOPIC_DOC)
    .define(LANGUAGE, Type.LIST, EMPTY_VALUE, Importance.MEDIUM, LANGUAGE_DOC)
    .define(OUTPUT_FORMAT, Type.STRING, OUTPUT_FORMAT_DEFAULT, Importance.MEDIUM, OUTPUT_FORMAT_DOC)
}

class TwitterSourceConfig(props: util.Map[String, String])
  extends AbstractConfig(TwitterSourceConfig.config, props) {
    getString(TwitterSourceConfig.STREAM_TYPE) match {
      case TwitterSourceConfig.STREAM_TYPE_SAMPLE => {}
      case TwitterSourceConfig.STREAM_TYPE_FILTER => {
        val terms = getList(TwitterSourceConfig.TRACK_TERMS)
        val locations = getList(TwitterSourceConfig.TRACK_LOCATIONS)
        val users = getList(TwitterSourceConfig.TRACK_FOLLOW)
        val language = getList(TwitterSourceConfig.LANGUAGE)
        if (terms.isEmpty && locations.isEmpty && users.isEmpty) {
          throw new RuntimeException("At least one of the parameters "
              + TwitterSourceConfig.TRACK_TERMS + ", " + TwitterSourceConfig.TRACK_LOCATIONS
              + ", " + TwitterSourceConfig.TRACK_FOLLOW + " should be specified!")
        }
        if (!locations.isEmpty) {
          if ((locations.size % 4) != 0) {
            throw new RuntimeException(TwitterSourceConfig.TRACK_LOCATIONS
                + " should have number of elements divisible by 4!")
          }
          try {
            locations.toList.map { x => x.trim.toDouble}
          } catch {
            case e: NumberFormatException => throw new RuntimeException("You should use double numbers in "
                + TwitterSourceConfig.TRACK_LOCATIONS)
          }
        }
        try {
            users.toList.map { x => x.trim.toLong}
        } catch {
          case e: NumberFormatException => throw new RuntimeException("You should use numeric user IDs in "
              + TwitterSourceConfig.TRACK_FOLLOW)
        }
      }
      case _ => throw new RuntimeException("Unknown value for "
          + TwitterSourceConfig.STREAM_TYPE + " parameter")
    }

}
