package com.eneco.trading.kafka.connect.twitter.domain

import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

object TwitterUser {
  def struct(u: twitter4j.User) =
    new Struct(schema)
      .put("id", u.getId)
      .put("name", u.getName)
      .put("screen_name", u.getScreenName)
      .put("location", u.getLocation)
      .put("verified", u.isVerified)
      .put("friends_count", u.getFriendsCount)
      .put("followers_count", u.getFollowersCount)
      .put("statuses_count", u.getStatusesCount)

  val schema = SchemaBuilder.struct().name("com.eneco.trading.kafka.connect.twitter.User")
    .field("id", Schema.INT64_SCHEMA)
    .field("name", Schema.OPTIONAL_STRING_SCHEMA)
    .field("screen_name", Schema.OPTIONAL_STRING_SCHEMA)
    .field("location", SchemaBuilder.string.optional.build())
    .field("verified", Schema.BOOLEAN_SCHEMA)
    .field("friends_count", Schema.INT32_SCHEMA)
    .field("followers_count", Schema.INT32_SCHEMA)
    .field("statuses_count", Schema.INT32_SCHEMA)
    .build()
}

object Entities {
  def struct(s: twitter4j.EntitySupport) =
    new Struct(schema)
      .put("hashtags", s.getHashtagEntities.toSeq.map(h =>
        new Struct(hschema)
          .put("text", h.getText)).asJava)
      .put("media", s.getMediaEntities.toSeq.map(m =>
        new Struct(mschema)
          .put("display_url", m.getDisplayURL)
          .put("expanded_url", m.getExpandedURL)
          .put("id", m.getId)
          .put("type", m.getType)
          .put("url", m.getURL)).asJava)
      .put("urls", s.getURLEntities.toSeq.map(u =>
        new Struct(uschema)
          .put("display_url", u.getDisplayURL)
          .put("expanded_url", u.getExpandedURL)
          .put("url", u.getURL)).asJava)
      .put("user_mentions", s.getUserMentionEntities.toSeq.map(um =>
        new Struct(umschema)
          .put("id", um.getId)
          .put("name", um.getName)
          .put("screen_name", um.getScreenName)).asJava)

  val hschema = SchemaBuilder.struct().name("com.eneco.trading.kafka.connect.twitter.Hashtag")
    .field("text", Schema.OPTIONAL_STRING_SCHEMA)
    .build()
  val mschema = SchemaBuilder.struct().name("com.eneco.trading.kafka.connect.twitter.Medium")
    .field("display_url", Schema.OPTIONAL_STRING_SCHEMA)
    .field("expanded_url", Schema.OPTIONAL_STRING_SCHEMA)
    .field("id", Schema.INT64_SCHEMA)
    .field("type", Schema.OPTIONAL_STRING_SCHEMA)
    .field("url", Schema.OPTIONAL_STRING_SCHEMA)
    .build()
  val uschema = SchemaBuilder.struct().name("com.eneco.trading.kafka.connect.twitter.Url")
    .field("display_url", Schema.OPTIONAL_STRING_SCHEMA)
    .field("expanded_url", Schema.OPTIONAL_STRING_SCHEMA)
    .field("url", Schema.OPTIONAL_STRING_SCHEMA)
    .build()
  val umschema = SchemaBuilder.struct().name("com.eneco.trading.kafka.connect.twitter.UserMention")
    .field("id", Schema.INT64_SCHEMA)
    .field("name", Schema.OPTIONAL_STRING_SCHEMA)
    .field("screen_name", Schema.OPTIONAL_STRING_SCHEMA)
    .build()

  val schema = SchemaBuilder.struct().name("com.eneco.trading.kafka.connect.twitter.Entities")
    .field("hashtags", SchemaBuilder.array(hschema).optional.build())
    .field("media", SchemaBuilder.array(mschema).optional.build())
    .field("urls", SchemaBuilder.array(uschema).optional.build())
    .field("user_mentions", SchemaBuilder.array(umschema).optional.build())
    .build()
}

object TwitterStatus {
  def asIso8601String(d:Date) = {
    val tz = TimeZone.getTimeZone("UTC")
    val df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
    df.setTimeZone(tz)
    df.format(if (d == null) { new Date() } else { d })
  }

  def struct(s: twitter4j.Status) =
    new Struct(schema)
      .put("id", s.getId)
      .put("created_at", asIso8601String(s.getCreatedAt))
      .put("user", TwitterUser.struct(s.getUser))
      .put("text", s.getText)
      .put("lang", s.getLang)
      .put("is_retweet", s.isRetweet)
      .put("entities", Entities.struct(s))

  val schema = SchemaBuilder.struct().name("com.eneco.trading.kafka.connect.twitter.Tweet")
    .field("id", Schema.INT64_SCHEMA)
    .field("created_at", Schema.OPTIONAL_STRING_SCHEMA)
    .field("user", TwitterUser.schema)
    .field("text", Schema.OPTIONAL_STRING_SCHEMA)
    .field("lang", Schema.OPTIONAL_STRING_SCHEMA)
    .field("is_retweet", Schema.BOOLEAN_SCHEMA)
    .field("entities", Entities.schema)
    .build()
}
