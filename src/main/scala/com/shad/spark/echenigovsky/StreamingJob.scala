package com.shad.spark.echenigovsky

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.joda.time.{LocalTime, Seconds => TimeSeconds}

object StreamingJob extends App {
  args match {
    case Array(
    zkQuorum,
    consumerGroupId,
    topicName,
    topicPartitions,
    checkpointDirectory) =>
      val conf = new SparkConf()
      val ssc = new StreamingContext(conf, Seconds(1))
      ssc.checkpoint(checkpointDirectory)

      lazy val startTime = new LocalTime()

      val kafkaStream = KafkaUtils.createStream(
        ssc = ssc,
        zkQuorum = zkQuorum,
        groupId = consumerGroupId,
        topics = Map(topicName -> topicPartitions.toInt)
      )

      kafkaStream.
        flatMapValues(StreamingCalculator.getCodeFromLog).
        filter { _._2 != "200" }.
        countByValueAndWindow(Seconds(60), Seconds(15)).
        map { case ((key, log), count) => s"60_second_count=$count" }.
        foreachRDD{rdd =>
          val seconds = TimeSeconds.secondsBetween(startTime, new LocalTime())
          println(f"${seconds.toStandardMinutes.getMinutes}m${seconds.getSeconds % 60}%02ds: ${rdd.take(1).headOption.getOrElse("60_second_count=0")}")
        }

      ssc.start()
      ssc.awaitTermination()

    case _ => throw new IllegalArgumentException(s"Wrong args: ${args.mkString(",")}")

  }
}
