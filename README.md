[![Build Status](https://travis-ci.org/Eneco/kafka-connect-twitter.svg?branch=master)](https://travis-ci.org/Eneco/kafka-connect-twitter)

Kafka Connect Twitter Source
============================

A Kafka Connect Source for Twitter. Using [Hosebird](https://github.com/twitter/hbc), tweets are received from the [Twitter Streaming API](https://dev.twitter.com/streaming/overview) which are fed into Kafka.

Setup
=====

Properties
----------

In addition to the default topics configuration the following options are added:

| name                     | data type | required | default | description                         |
|:-------------------------|:----------|:---------|:--------|:------------------------------------|
| `twitter.consumerkey`    | string    | yes      |         | Twitter consumer key                |
| `twitter.consumersecret` | string    | yes      |         | Twitter consumer secret             |
| `twitter.token`          | string    | yes      |         | Twitter token                       |
| `twitter.secret`         | string    | yes      |         | Twitter secret                      |
| `stream.type`            | string    | no       | filter  | Type of stream ¹                    |
| `track.terms`            | string    | maybe ²  |         | A Twitter `track` parameter ²       |
| `track.locations`        | string    | maybe ²  |         | A Twitter `locations` parameter ³   |
| `track.follow`           | string    | maybe ²  |         | A Twitter `follow` parameter ⁴      |
| `batch.size`             | int       | no       | 100     | Flush after this many tweets ⁶      |
| `batch.timeout`          | double    | no       | 0.1     | Flush after this many seconds ⁶     |
| `language`               | string    | no       |         | List of languages to fetch ⁷        |

¹ Type of stream: [filter](https://dev.twitter.com/streaming/reference/post/statuses/filter), or [sample](https://dev.twitter.com/streaming/reference/get/statuses/sample).

² When the `filter` type is used, one of the parameters `track.terms`, `track.locations`, or `track.follow` should be specified.  If multiple parameters are specified, they are working as OR operation.

³ Please refer to [here](https://dev.twitter.com/streaming/overview/request-parameters#track) for the format of the `track` parameter.

⁴ Please refer to [here](https://dev.twitter.com/streaming/overview/request-parameters#locations) for the format of the `locations` parameter.

⁵ Please refer to [here](https://dev.twitter.com/streaming/overview/request-parameters#follow) for the format of the `follow` parameter.

⁶ Tweets are accumulated and flushed as a batch into Kafka; when the batch is larger than `batch.size` or when the oldest tweet in it is older than `batch.timeout` [s], it is flushed.

⁷ List of languages for which tweets will be returned. Can be used with any stream type.  See [here](https://dev.twitter.com/streaming/overview/request-parameters#language) for format of the `language` parameter.

An example `twitter-source.properties`:

    name=twitter-source
    connector.class=com.eneco.trading.kafka.connect.twitter.TwitterSourceConnector
    tasks.max=1
    topic=twitter
    twitter.consumerkey=(secret)
    twitter.consumersecret=(secret)
    twitter.token=(secret)
    twitter.secret=(secret)
    track.terms=test

Creating a Twitter application
------------------------------

Visit https://apps.twitter.com/ and `Create a New App`. Fill in an application name & description & web site and accept the developer aggreement. Click on `Create my access token` and populate a file `twitter-source.properties` with consumer key & secret and the access token & token secret using the example file to begin with.

Setting up the Confluent Platform
---------------------------------

Follow instructions at [Confluent](http://docs.confluent.io) and install and run the `schema-registry` service, and appropriate `zookeeper` & `kafka` brokers. Once the platform is up & running, populate the file `connect-standalone.properties` with the appropriate hostnames and ports.

Assuming that `$CONFLUENT_HOME` refers to the root of your Confluent Platform installation:

Start Zookeeper:

    $CONFLUENT_HOME/bin/zookeeper-server-start $CONFLUENT_HOME/etc/kafka/zookeeper.properties

Start Kafka:

    $CONFLUENT_HOME/bin/kafka-server-start $CONFLUENT_HOME/etc/kafka/server.properties

Start the Schema Registry:

    $CONFLUENT_HOME/bin/schema-registry-start $CONFLUENT_HOME/etc/schema-registry/schema-registry.properties

Starting kafka-connect-twitter
------------------------------

Having cloned this repository, build the latest source code with:

    mvn clean package

Put the JAR file location into your `CLASSPATH`:

    export CLASSPATH=`pwd`/target/kafka-connect-twitter-0.1-jar-with-dependencies.jar

Start a Kafka Connect instance:

    $CONFLUENT_HOME/bin/connect-standalone connect-standalone.properties twitter-source.properties 

And watch tweets come in as JSON:

    $CONFLUENT_HOME/bin/kafka-avro-console-consumer --topic twitter --zookeeper localhost:2181

Alternatively, if you have [jq](https://stedolan.github.io/jq/) installed, see the tweets pretty printed:

    $CONFLUENT_HOME/bin/kafka-avro-console-consumer --topic twitter --zookeeper localhost:2181 | jq

Work in progress!

Todo:
-----
-   Add hosebird client mode to take the full fat response rather than the twitter4j subset. Needs json to Avro converter. Avro4s?
-   Split the track terms up and assign to workers? Limits on connections to twitter?
-   [ ] Extend
-   [ ] Test
-   [ ] Document

