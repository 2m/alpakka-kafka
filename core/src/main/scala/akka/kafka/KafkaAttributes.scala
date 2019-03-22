/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka
import akka.stream.Attributes.Attribute

object KafkaAttributes {

  case class TransactionalCopyRunId(id: String) extends Attribute

}
