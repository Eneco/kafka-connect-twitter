[![Build Status](https://travis-ci.org/Eneco/kafka-connect-twitter.svg?branch=master)](https://travis-ci.org/Eneco/kafka-connect-twitter)

# Kafka Connect Twitter Source
Kafka Connect Source for Twitter

# Creating a Twitter application

Visit https://apps.twitter.com/ and `Create a New App`. Fill in an application name & description & web site and accept the developer aggreement. Click on ``Create my access token`` and populate a file ``twitter-source.properties`` with consumer key & secret and the access token & token secret using the example file to begin with.

# Setting up the Confluent Platform

Follow instructions at http://docs.confluent.io and install and run the `schema-registry` service, and appropriate ``zookeeper`` & ``kafka`` brokers. Once the platform is up & running, populate the file ``connect-standalone.properties`` with the appropriate hostnames and ports. 

# Starting kafka-connect-twitter

Having cloned this github repository, build the latest source code with:

    $ mvn clean install

And then execute the JAR (with dependencies) by passing in the server property file & the twitter property file 

    $ java -jar target/kafka-connect-twitter-0.1-jar-with-dependencies.jar --server connect-standalone.properties --twitter  

Work in progress!

## Todo:
 - [ ] Extend
 - [ ] Test
 - [ ] Document
