package bigdata.spark.sparksql
import org.apache.phoenix.spark._

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql._

object phoenixIntegration {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("phoenixIntegration").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("phoenixIntegration").enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql

    val df = spark.read.format("org.apache.phoenix.spark").option("table","dept").option("zkUrl","localhost:2181").load()
     df.show()
    spark.sql("create database anothertestdb")
    // Save the Hbase data into Hive:
    df.write.format("hive").saveAsTable("anothertestdb.dhartable")
    spark.stop()
  }
}