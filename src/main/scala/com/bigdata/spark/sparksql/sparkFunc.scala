package com.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object sparkfunctions {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("sparkfunctions").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data = "C:\\work\\dataset\\stocks.json"
    val df = spark.read.format("json").load(data)
    //below is a REGULAR EXPRESSION; it will convert all the special characters like hypen, slash
    val reg = "[^\\p{L}\\p{Nd}]+"
    val cols = df.columns.map(x=>x.replaceAll(reg,""))
    //toDF() used convert rdd to dataframe, 2) rename all columns
    val ndf = df.toDF(cols:_*)
    ndf.show()
    ndf.printSchema()
    //ndf.show()
    // Rename all the columns
    // val newdf = df.toDF("name","g","k","m","model","price","sty")
    // Rename perticuler column
    //val ndf1 = ndf.withColumnRenamed("ContryName","Country")
    //ndf.createOrReplaceTempView("tab")
    //val res = spark.sql("select price+nvl(Gears,0) sumtocols,nvl(Gears,0) gears,price from tab")
    //res.show()
    // Update data in the perticuler column
    //val upd = ndf.withColumn("Gears", when($"Gears".isNull,0).otherwise($"Gears"))
    //df.select($"name",reverse($"email").alias("reverseemail")).show()
    //ndf.select($"CountryName",reverse($"price").alias("reverseprice")).show()

    //ndf.createOrReplaceTempView("tab")
    //val res = spark.sql("select 52WeekLow+nvl(AnalystRecom,0) sumtwocols, 52WeekLow, nvl(AnalystRecom,0) AnalystRecom from tab ")
    // res.show()
    ndf.createOrReplaceTempView("tab")
    // val res = spark.sql("select 52WeekLow+nvl(AnalystRecom,0) sumtwocols, 52WeekLow, nvl(AnalystRecom,0) AnalystRecom from tab ")

    val upd = ndf.withColumn("AnalystRecom", when($"AnalystRecom".isNull,0).otherwise($"AnalystRecom")).withColumn("20DaySimpleMovingAverage", abs($"20DaySimpleMovingAverage"))withColumn("Beta", when($"Beta".isNull,0).otherwise($"Beta"))
    upd.show()
    spark.stop()
  }
}