package com.eneco.trading.kafka.connect.twitter

/**
  * Created by andrew@datamountaineer.com on 24/02/16. 
  * kafka-connect-twitter
  */

import org.slf4j.LoggerFactory

trait Logging {
  val loggerName = this.getClass.getName
  @transient lazy val log = LoggerFactory.getLogger(loggerName)
}