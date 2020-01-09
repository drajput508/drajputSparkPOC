package com.scala.scalaExamples

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql._

object firstClass {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("firstClass").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("firstClass").getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql

    spark.stop()
  }
}

