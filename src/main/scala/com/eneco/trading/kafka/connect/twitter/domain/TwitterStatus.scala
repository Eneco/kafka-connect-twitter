package com.eneco.trading.kafka.connect.twitter.domain

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
  def apply(s: Status) = {
    new TwitterStatus(id = s.getId, createdAt = s.getCreatedAt.toString, favoriteCount = s.getFavoriteCount, text = s.getText, user = TwitterUser(s.getUser))
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
