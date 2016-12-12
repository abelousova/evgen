package com.shad.spark.echenigovsky

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.LocalDate

object LikesCounterJob extends App {
  val inputBasePath = "/user/sandello/logs/access.log."
  val outputBasePath = "/user/echernigovsky/hw3/profile_liked_three_days"
  args match {
    case Array(date) =>
      val conf = new SparkConf()
      implicit val sc = new SparkContext(conf)
      implicit val sqlContext = new SQLContext(sc)
      import sqlContext.implicits._

      val localDate = new LocalDate(date)
      val outputPath = s"$outputBasePath/$date"

      val count = if (localDate.isBefore(new LocalDate("2016-10-09"))) {
        0
      } else {
        val paths = (-2 to 0).
          map(i => localDate.minusDays(i)).
          map(date => s"$inputBasePath${date.toString}")

        val threeDaysDataset = paths.
          zipWithIndex.
          map { case (path, idx) =>
            sqlContext.read.text(path)
              .withColumnRenamed("value", "line")
              .withColumn("index", lit(idx))
          }.
          reduce(_.unionAll(_)).
          as[Line]

        LikesCounter.countIdsWithThreeDaysLikes(threeDaysDataset)
      }

      sc.parallelize(Seq(count.toString), 1).toDF.write.text(outputPath)

    case _ => throw new IllegalArgumentException(s"Wrong args: ${args.mkString(",")}")
  }
}

