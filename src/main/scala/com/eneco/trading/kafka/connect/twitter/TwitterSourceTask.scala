package com.eneco.trading.kafka.connect.twitter

import java.util
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}
import scala.collection.JavaConversions._

/**
  * Created by andrew@datamountaineer.com on 24/02/16. 
  * kafka-connect-twitter
  */
class TwitterSourceTask extends SourceTask with Logging {
  private var reader : Option[TwitterStreamReader] = null

  override def poll(): util.List[SourceRecord] = {
    require(reader.isDefined, "Twitter client not initialized!")
    reader.get.poll()
  }

  override def start(props: util.Map[String, String]): Unit = {
    TwitterSourceConfig.config.parse(props)
    val sourceConfig = new TwitterSourceConfig(props)
    reader = Some(TwitterReader(config = sourceConfig))
  }

  override def stop() = {
    reader.foreach(r=>r.stop())
  }
  override def version(): String = ""
}
