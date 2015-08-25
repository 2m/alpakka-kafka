package com.softwaremill.react.kafka

/**
 * Based on from https://github.com/cjdev/kafka-rx
 */
package object commit {

  import org.apache.curator.utils.ZKPaths
  type Offsets = Map[TopicPartition, Long]
  type TopicPartition = (String, Int)
  type OffsetMerge = (Offsets, Offsets) => Offsets

  private[commit] val defaultMerge: OffsetMerge = { case (theirs, ours) => ours }
  private[commit] val defaultOffsets = Map[TopicPartition, Long]()

  private[commit] def getPartitionPath(group: String, topic: String, part: Int) =
    ZKPaths.makePath(s"/consumers/$group/offsets/$topic", part.toString)
}