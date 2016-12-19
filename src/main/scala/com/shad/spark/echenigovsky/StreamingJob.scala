package com.shad.spark.echenigovsky

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.joda.time.{LocalTime, Seconds => TimeSeconds}

import scala.collection.mutable

object StreamingJob extends App {
  args match {
    case Array(
    zkQuorum,
    consumerGroupId,
    topicName,
    topicPartitions,
    checkpointDirectory) =>
      val conf = new SparkConf()
      val sc = new SparkContext(conf)
      val ssc = new StreamingContext(sc, Seconds(1))
      ssc.checkpoint(checkpointDirectory)

      val kafkaStream = KafkaUtils.createStream(
        ssc = ssc,
        zkQuorum = zkQuorum,
        groupId = consumerGroupId,
        topics = Map(topicName -> topicPartitions.toInt)
      )

      var currentTime = 0l

      var totalValue = 0l
      val minuteValues = mutable.Queue.empty[Long]

      kafkaStream.
        flatMapValues(StreamingCalculator.getCodeFromLog).
        filter { _._2 != "200" }.
        countByValueAndWindow(Seconds(15), Seconds(15)).
        map { case ((key, code), count) => count }.
        foreachRDD{rdd =>
          val currentValue = rdd.take(1).headOption.getOrElse(0l)

          if (minuteValues.size > 3) {
            minuteValues.dequeue()
          }
          minuteValues.enqueue(currentValue)

          totalValue += currentValue

          println(
            f"""${currentTime / 60}m${currentTime % 60}%02ds :
               |  "15_second_count=$currentValue;
               |  60_second_count=${minuteValues.sum};
               |  total_count=$totalValue;"
               | """.stripMargin)

          currentTime += 15
        }

      ssc.start()
      ssc.awaitTermination()

    case _ => throw new IllegalArgumentException(s"Wrong args: ${args.mkString(",")}")

  }
}
