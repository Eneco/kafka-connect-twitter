[![Build Status](https://travis-ci.org/Eneco/kafka-connect-twitter.svg?branch=master)](https://travis-ci.org/Eneco/kafka-connect-twitter)

Kafka Connect Twitter
=====================

A Kafka Connect for Twitter. Both a source (from Twitter to Kafka) and sink (from Kafka to Twitter) are provided:

-   The *sink* receives plain strings from Kafka, which are tweeted using [Twitter4j](http://twitter4j.org/);
-   The *source* receives tweets from the [Twitter Streaming API](https://dev.twitter.com/streaming/overview) using [Hosebird](https://github.com/twitter/hbc), which are fed into Kafka either as a `TwitterStatus` structure (default) or as plain strings.

Data types
==========

-   The *sink* connector expects plain strings (UTF-8 by default) from Kafka (`org.apache.kafka.connect.storage.StringConverter`), i.e. `kafka-console-producer` will do;
-   The *source* connector either outputs `TwitterStatus` structures (default) or plain strings. The Kafka Connect framework is serialization format agnostic. An intermidiate representation is used inside the framework; when an actual Kafka record is to be created, the `key.converter` and `value.converter` properties are used. Chances are that you use Avro (`io.confluent.connect.avro.AvroConverter`) or JSON (`org.apache.kafka.connect.json.JsonConverter`). When `output.format=string`, both the key and value are strings, with the key the user name and the value the tweet text. Here the `org.apache.kafka.connect.storage.StringConverter` converter must be used.

An actual `TwitterStatus` after JSON conversion, freshly grabbed from Kafka, looks like:

``` json
{
  "id": 723416534626881536,
  "createdAt": "Fri Apr 22 09:41:56 CEST 2016",
  "favoriteCount": 0,
  "text": "Pipio ergo sum",
  "user": {
    "id": 4877511249,
    "name": "rollulus",
    "screenName": "rollulus"
  }
}
```

(indeed, having the `favoriteCount` field in there was a totally arbitrary choice)

Setup
=====

Properties
----------

In addition to the default configuration for Kafka connectors (e.g. `name`, `connector.class`, etc.) the following options are needed for both the source and sink:

| name                     | data type | required | default | description             |
|:-------------------------|:----------|:---------|:--------|:------------------------|
| `twitter.consumerkey`    | string    | yes      |         | Twitter consumer key    |
| `twitter.consumersecret` | string    | yes      |         | Twitter consumer secret |
| `twitter.token`          | string    | yes      |         | Twitter token           |
| `twitter.secret`         | string    | yes      |         | Twitter secret          |

This is all for the sink. The *source* has the following additional properties:

| name              | data type | required | default      | description                                |
|:------------------|:----------|:---------|:-------------|:-------------------------------------------|
| `stream.type`     | string    | no       | filter       | Type of stream ¹                           |
| `track.terms`     | string    | maybe ²  |              | A Twitter `track` parameter ²              |
| `track.locations` | string    | maybe ²  |              | A Twitter `locations` parameter ³          |
| `track.follow`    | string    | maybe ²  |              | A Twitter `follow` parameter ⁴             |
| `batch.size`      | int       | no       | 100          | Flush after this many tweets ⁶             |
| `batch.timeout`   | double    | no       | 0.1          | Flush after this many seconds ⁶            |
| `language`        | string    | no       |              | List of languages to fetch ⁷               |
| `output.format`   | string    | no       | `structured` | The output format: `[structured|string]` ⁸ |

¹ Type of stream: [filter](https://dev.twitter.com/streaming/reference/post/statuses/filter), or [sample](https://dev.twitter.com/streaming/reference/get/statuses/sample).

² When the `filter` type is used, one of the parameters `track.terms`, `track.locations`, or `track.follow` should be specified. If multiple parameters are specified, they are working as OR operation.

³ Please refer to [here](https://dev.twitter.com/streaming/overview/request-parameters#track) for the format of the `track` parameter.

⁴ Please refer to [here](https://dev.twitter.com/streaming/overview/request-parameters#locations) for the format of the `locations` parameter.

⁵ Please refer to [here](https://dev.twitter.com/streaming/overview/request-parameters#follow) for the format of the `follow` parameter.

⁶ Tweets are accumulated and flushed as a batch into Kafka; when the batch is larger than `batch.size` or when the oldest tweet in it is older than `batch.timeout` [s], it is flushed.

⁷ List of languages for which tweets will be returned. Can be used with any stream type. See [here](https://dev.twitter.com/streaming/overview/request-parameters#language) for format of the `language` parameter.

⁸ The source can output in two ways: *structured*, where a `TwitterStatus` structures are output as values, or *string*, where both the key and value are strings, with the key the user name and the value the tweet text. Remember to update `key.converter` and `value.converter` appropriately: `io.confluent.connect.avro.AvroConverter` or `org.apache.kafka.connect.json.JsonConverter` for *structured*; `org.apache.kafka.connect.storage.StringConverter` for *string*.

An example `twitter-source.properties`:

``` properties
name=twitter-source
connector.class=com.eneco.trading.kafka.connect.twitter.TwitterSourceConnector
tasks.max=1
topic=twitter
twitter.consumerkey=(secret)
twitter.consumersecret=(secret)
twitter.token=(secret)
twitter.secret=(secret)
track.terms=test
```

And an example `twitter-sink.properties` is like:

``` properties
name=twitter-sink
connector.class=com.eneco.trading.kafka.connect.twitter.TwitterSinkConnector
tasks.max=1
topics=texts-to-tweet
twitter.consumerkey=(secret)
twitter.consumersecret=(secret)
twitter.token=(secret)
twitter.secret=(secret)
```

Creating a Twitter application
------------------------------

To obtain the required keys, visit https://apps.twitter.com/ and `Create a New App`. Fill in an application name & description & web site and accept the developer aggreement. Click on `Create my access token` and populate a file `twitter-source.properties` with consumer key & secret and the access token & token secret using the example file to begin with.

Setting up the Confluent Platform
---------------------------------

Follow instructions at [Confluent](http://docs.confluent.io) and install and run the `schema-registry` service, and appropriate `zookeeper` & `kafka` brokers. Once the platform is up & running, populate the file `connect-sink-standalone.properties` and / or `connect-source-standalone.properties` with the appropriate hostnames and ports.

Assuming that `$CONFLUENT_HOME` refers to the root of your Confluent Platform installation:

Start Zookeeper:

    $CONFLUENT_HOME/bin/zookeeper-server-start $CONFLUENT_HOME/etc/kafka/zookeeper.properties

Start Kafka:

    $CONFLUENT_HOME/bin/kafka-server-start $CONFLUENT_HOME/etc/kafka/server.properties

Start the Schema Registry:

    $CONFLUENT_HOME/bin/schema-registry-start $CONFLUENT_HOME/etc/schema-registry/schema-registry.properties

Running
=======

Starting kafka-connect-twitter
------------------------------

Having cloned this repository, build the latest source code with:

    mvn clean package

Put the JAR file location into your `CLASSPATH`:

    export CLASSPATH=`pwd`/target/kafka-connect-twitter-0.1-jar-with-dependencies.jar

### Source, structured output mode

To start a Kafka Connect source instance:

    $CONFLUENT_HOME/bin/connect-standalone connect-source-standalone.properties twitter-source.properties 

And watch Avro `TwitterStatus` tweets come in represented as JSON:

    $CONFLUENT_HOME/bin/kafka-avro-console-consumer --topic twitter --zookeeper localhost:2181

### Source, simple (plain strings) output mode

To start a Kafka Connect source instance:

    $CONFLUENT_HOME/bin/connect-standalone connect-simple-source-standalone.properties twitter-simple-source.properties

And watch tweets come in, with the key the user, and the value the tweet text:

    $CONFLUENT_HOME/bin/kafka-console-consumer --zookeeper localhost:2181 \
          --topic twitter \
          --formatter kafka.tools.DefaultMessageFormatter \
          --property print.key=true \
          --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
          --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer

### Sink

To start a Kafka Connect sink instance:

    $CONFLUENT_HOME/bin/connect-standalone connect-sink-standalone.properties twitter-sink.properties 

Fire up the console producer to feed text from the console into your topic:

    $CONFLUENT_HOME/bin/kafka-console-producer -broker-list localhost:9092 --topic texts-to-tweet
    Pipio ergo sum

Todo:
-----

-   Add hosebird client mode to take the full fat response rather than the twitter4j subset. Needs json to Avro converter. Avro4s?
-   Split the track terms up and assign to workers? Limits on connections to twitter?
-   [ ] Extend
-   [ ] Test
-   [ ] Document

