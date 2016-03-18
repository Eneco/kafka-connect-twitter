package com.eneco.trading.kafka.connect.twitter

import java.util
import org.apache.kafka.connect.connector.{Task, Connector}
import org.apache.kafka.connect.errors.ConnectException
import scala.collection.JavaConverters._
import scala.util.{Failure, Try}

/**
  * Created by andrew@datamountaineer.com on 24/02/16. 
  * kafka-connect-twitter
  */
class TwitterSourceConnector extends Connector with Logging {
  private var configProps : util.Map[String, String] = null

  /**
    * States which SourceTask class to use
    * */
  override def taskClass(): Class[_ <: Task] = classOf[TwitterSourceTask]

  /**
    * Set the configuration for each work and determine the split
    *
    * @param maxTasks The max number of task workers be can spawn
    * @return a List of configuration properties per worker
    * */
  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = {
    log.info(s"Setting task configurations for $maxTasks workers.")
    (1 to maxTasks).map(c => configProps).toList.asJava
  }

  /**
    * Start the source and set to configuration
    *
    * @param props A map of properties for the connector and worker
    * */
  override def start(props: util.Map[String, String]): Unit = {
    log.info(s"Starting Twitter source task with ${props.toString}.")
    configProps = props
    Try(new TwitterSourceConfig(props)) match {
      case Failure(f) => throw new ConnectException("Couldn't start Twitter source due to configuration error: "
          + f.getMessage, f)
      case _ =>
    }
  }

  override def stop() = {}
  override def version(): String = ""
}
