FROM eneco/connector-base:0.1.0

COPY target/kafka-connect-twitter-0.1-jar-with-dependencies.jar /etc/kafka-connect/jars
