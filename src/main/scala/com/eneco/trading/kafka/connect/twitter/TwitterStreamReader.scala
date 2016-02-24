package com.eneco.trading.kafka.connect.twitter

import com.twitter.hbc.httpclient.BasicClient
import org.apache.kafka.connect.source.SourceRecord

/**
  * Created by andrew@datamountaineer.com on 24/02/16. 
  * kafka-connect-twitter
  */
class TwitterStreamReader(client: BasicClient) extends Logging {
  log.info("Initialising Twitter Stream Reader")
  client.connect()


  def poll() : List[SourceRecord] = ???

  /**
    * Stop the HBC client
    * */
  def stop() = {
    client.stop()
  }
}
