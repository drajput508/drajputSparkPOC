package bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql._

object ReadXML {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("ReadXML").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("ReadXML").getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql

    val data = "C:\\work\\dataset\\books.xml"

    var df = spark.read.format("xml").option("rowTag","book").load(data)
    //df.show()
    df.createOrReplaceTempView("book")

    val res = spark.sql("select * from book where price > 30")
    res.show()

    spark.stop()
  }
}