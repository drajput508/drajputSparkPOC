package bigdata.spark.sparksql

import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._

object oracleToHive {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("oracleToHive").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("oracleToHive").enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val tab = args(0)
    val host = "jdbc:oracle:thin:@//mforacle.cj3qjsgo9lr7.ap-south-1.rds.amazonaws.com:1521/orcl"
    val prop = new Properties()
    prop.setProperty("user","ousername")
    prop.setProperty("password","opassword")
    prop.setProperty("driver","oracle.jdbc.OracleDriver")

    val res = spark.read.jdbc(host, "EMP",prop)
    res.write.mode(SaveMode.Overwrite).format("hive").saveAsTable(tab)

    spark.stop()
  }
}