package com.shad.spark.echenigovsky

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.kafka.KafkaUtils

object StreamingJob extends App {
  args match {
    case Array(
      zkQuorum,
      consumerGroupId,
      topicName,
      topicPartitions) =>
      val conf = new SparkConf()
      val ssc = new StreamingContext(conf, Seconds(1))

      val kafkaStream = KafkaUtils.createStream(
        ssc = ssc,
        zkQuorum = zkQuorum,
        groupId = consumerGroupId,
        topics = Map(topicName -> topicPartitions.toInt)
      )

      kafkaStream.
        flatMapValues(StreamingCalculator.getCodeFromLog).
        filter{_._2 != "200"}.
        countByWindow(Seconds(60), Seconds(15)).
        map(count => s"60_second_count=$count")
        print()

    case _ => throw new IllegalArgumentException(s"Wrong args: ${args.mkString(",")}")

  }
}
