package com.scala.scalaExamples

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql._

object learningVar {
  def main(args: Array[String]) {
    var a = 5
    var b = 6

    var maxVarSqrRoot = if (a > b) {
      var sqrRoot = a * a
      sqrRoot * 2
    }
    else {
      var sqrRoot = b * b
      sqrRoot * 2
    }
    print(maxVarSqrRoot)
  }
}

