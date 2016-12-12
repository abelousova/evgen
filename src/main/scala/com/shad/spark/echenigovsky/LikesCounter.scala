package com.shad.spark.echenigovsky

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SQLContext}

import scala.util.matching.Regex

object LikesCounter {

  def countIdsWithThreeDaysLikes(input: Dataset[Line])(implicit sqlContext: SQLContext): Long = {
    import sqlContext.implicits._

    val regex = sqlContext.sparkContext.broadcast(
      new Regex("([\\d\\.:]+) - - \\[(\\S+) [^\"]+\\] \"(\\w+) /id(\\d+)\\?like=1 (HTTP/[\\d\\.]+)\" (\\d+) \\d+ \"([^\"]+)\" \"([^\"]+)\"")
    )

    input.
      flatMap(row => parseLine(row, regex.value)).
      filter(_.code == "200").
      groupBy(_.id).
      agg(countDistinct("index").alias("count").as[Long]).
      filter(_._2 == 3).
      count
  }

  def parseLine(line: Line, regex: Regex): Option[Info] = {
    val matched = regex.findFirstMatchIn(line.line)
    matched.map(group => Info(
      id = group.group(4),
      code = group.group(6),
      index = line.index
    ))
  }

}

case class Line(line: String, index: Int)

case class Info(id: String, index: Int, code: String)
