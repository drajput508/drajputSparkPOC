package bigdata.spark.sparksql

import org.apache.spark.sql._

import org.apache.spark.sql._

object getOracleData {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("getOracleData").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("getOracleData").getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql

    val host = "jdbc:oracle:thin:@//mforacle.cj3qjsgo9lr7.ap-south-1.rds.amazonaws.com:1521/orcl"
    val prop = new java.util.Properties()
    prop.setProperty("user","ousername")
    prop.setProperty("password","opassword")
    prop.setProperty("driver","oracle.jdbc.OracleDriver")

    //  if mysql use the driver :  com.mysql.cj.jdbc.Driver
    //  if mssql use the driver:   com.microsoft.sqlserver.jdbc.SQLServerDriver

    // Get data directly from the table
    val df = spark.read.jdbc(host,"EMP",prop)
    df.show()


    // Get data based on query
    val query = "(select * from EMP where sal>2500) tmp"
    val dfq = spark.read.jdbc(host,query,prop)
    dfq.show()

    spark.stop()


  }
}