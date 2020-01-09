package bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql._

//bigdata.spark.sparksql.writeToOracle
object writeToOracle {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("writeToOracle").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("writeToOracle").getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql

    //val data = "C:\\work\\dataset\\bank-full.csv"
    val data = args(0)
    val tab = args(1)
    val df = spark.read.format("csv").option("header","true").option("delimiter",";").load(data)
    df.createOrReplaceTempView("bank")
    val res = spark.sql("select * from Bank where age > 10")

    val host = "jdbc:oracle:thin:@//mforacle.cj3qjsgo9lr7.ap-south-1.rds.amazonaws.com:1521/orcl"
    val prop = new java.util.Properties()
    prop.setProperty("user","ousername")
    prop.setProperty("password","opassword")
    prop.setProperty("driver","oracle.jdbc.OracleDriver")

    res.write.mode(SaveMode.Overwrite).jdbc(host,tab,prop)

    spark.stop()
  }
}


