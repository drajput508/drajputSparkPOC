package bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql._

object complexJsonData {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("complexJsonData").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("complexJsonData").getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data = "C:\\work\\dataset\\world_bank.json"
    val df = spark.read.format("json").option("inferSchema","true").load(data)
    df.createOrReplaceTempView("tab")

   /* // only struct datatype , use ... parant_column.child_column
    |-- theme1: struct (nullable = true)                  <----------------
      |    |-- Name: string (nullable = true)
    |    |-- Percent: long (nullable = true)
*/
    val res = spark.sql("select url,totalamt, theme1.name theme1Name,theme1.percent theme1percent from tab")
    res.show()
/*
    // use `` tild simbol if we have any spectial character in the column name
    |-- _id: struct (nullable = true)
    |    |-- $oid: string (nullable = true)
*/
    val res1 = spark.sql("select _id.`$oid` oid, url,totalamt, theme1.name theme1Name,theme1.percent theme1percent from tab")
    res1.show()

    // use below function when you have schema like:   Array--> Struct
    val res2 = spark.sql("select _id.`$oid` oid, url,totalamt, theme1.name theme1Name,theme1.percent theme1percent, mp.Name mpname,mp.percent mppercent from tab lateral view explode(majorsector_percent) t as mp")
    res2.show()
    spark.stop()
  }
}