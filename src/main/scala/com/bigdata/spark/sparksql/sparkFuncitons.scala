package bigdata.spark.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object sparkFuncitons {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("sparkFuncitons").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("sparkFuncitons").getOrCreate()
    val sc = spark.sparkContext
    Logger.getLogger("org").setLevel(Level.INFO)
    Logger.getLogger("akka").setLevel(Level.INFO)

    import spark.implicits._
    import spark.sql
    val data = "C:\\work\\dataset\\stocks.json"
    val regx = "[^\\p{L}\\p{Nd}]+"
    val df = spark.read.format("json").option("dropFieldIfAllNull","true").load(data)
    //val cols = df.columns.map(x=>x.replaceAll(" ",""))
    //If you want to remove special character than use regex
    val colsRegx = df.columns.map(x=>x.replaceAll(regx,""))

    //toDF() used to (1) convert RDD to df. (2) to rename the columns
    val ndf = df.toDF(colsRegx:_*)

    val upd = ndf.withColumn("AnalystRecom", when($"AnalystRecom".isNull,0).otherwise($"AnalystRecom")).withColumn("20DaySimpleMovingAverage", abs($"20DaySimpleMovingAverage"))
    upd.show()

    spark.stop()
  }
}