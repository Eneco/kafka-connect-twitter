package com.eneco.trading.kafka.connect.twitter.domain

import java.text.{DateFormat, SimpleDateFormat}
import java.time.{LocalDate, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.util.{TimeZone, Date}

import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import twitter4j.{Status, User}

/**
  * Created by andrew@datamountaineer.com on 29/02/16. 
  * kafka-connect-twitter
  */
case class TwitterStatus(id: Long, createdAt: String, favoriteCount: Int, text: String, user: TwitterUser)
case class TwitterUser(id: Long, name: String, screenName: String)

object TwitterUser {
  def apply(u: User) = {
    new TwitterUser(id = u.getId, name = u.getName, screenName = u.getScreenName)
  }

  def struct(u: TwitterUser) =
    new Struct(schema)
      .put("id", u.id)
      .put("name", u.name)
      .put("screenName", u.screenName)

  val schema = SchemaBuilder.struct().name("com.eneco.trading.kafka.connect.twitter.TwitterUser")
    .field("id", Schema.INT64_SCHEMA)
    .field("name", Schema.STRING_SCHEMA)
    .field("screenName", Schema.STRING_SCHEMA)
    .build()
}

object TwitterStatus {
  def asIso8601String(d:Date) = {
    val tz = TimeZone.getTimeZone("UTC")
    val df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
    df.setTimeZone(tz)
    df.format(new Date())
  }

  def apply(s: Status) = {
    new TwitterStatus(id = s.getId, createdAt = asIso8601String(s.getCreatedAt), favoriteCount = s.getFavoriteCount, text = s.getText, user = TwitterUser(s.getUser))
  }

  def struct(s: TwitterStatus) =
    new Struct(schema)
      .put("id", s.id)
      .put("createdAt", s.createdAt)
      .put("favoriteCount", s.favoriteCount)
      .put("text", s.text)
      .put("user", TwitterUser.struct(s.user))

  val schema = SchemaBuilder.struct().name("com.eneco.trading.kafka.connect.twitter.TwitterStatus")
    .field("id", Schema.INT64_SCHEMA)
    .field("createdAt", Schema.STRING_SCHEMA)
    .field("favoriteCount", Schema.INT32_SCHEMA)
    .field("text", Schema.STRING_SCHEMA)
    .field("user", TwitterUser.schema)
    .build()
}
