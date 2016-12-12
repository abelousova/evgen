package com.shad.spark.echenigovsky

import scala.util.matching.Regex

object StreamingCalculator {
  def getCodeFromLog(line: String): Option[String] = {
    val regex = new Regex("([\\d\\.:]+) - - \\[(\\S+) [^\"]+\\] \"(\\w+) /(\\w+) (HTTP/[\\d\\.]+)\" (\\d+) \\d+ \"([^\"]+)\" \"([^\"]+)\"")

    val m = regex.findFirstMatchIn(line)
    m.map(_.group(6))
  }
}
