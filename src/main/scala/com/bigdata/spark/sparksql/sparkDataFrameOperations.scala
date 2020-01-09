package bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql._

object sparkDataFrameOperations {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("sparkDataFrameOperations").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("sparkDataFrameOperations").getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql

    val data = "C:\\work\\dataset\\bank-full.csv"

    val df= spark.read.format("csv").option("header","true").option("delimiter",";").option("inferSchema","true").load(data)
    //df.show()
    // where($"age") ... if you mention $"age" $ represent Column ..
    // as per scala standards "anything in double quotes called" string.
    //scala, java dsl function .. if you don't know sql .. use this function
    val res = df.select("*").where($"age">80 && $"marital".equals("married"))
    //df.groupBy($"marital").agg(mean($"balance").as("avg_bank_bal"))
    df.groupBy($"marital").min()
    res.show(49)
    res.createOrReplaceTempView("tab")
    val ndf1 = df
    df.join(ndf1)
    res.withColumnRenamed("age","newage")


    spark.stop()
  }
}