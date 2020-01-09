package bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql._

object sparkhivejson {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("sparkhivejson").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").enableHiveSupport().appName("sparkhivejson").getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val input = args(0)
    val tab = args(1)
    val df = spark.read.format("json").option("inferSchema","true").load(input)
    df.write.format("hive").saveAsTable(tab)
    val pro = spark.sql ("select city, loc[0] lang, loc[1] lati, pop, state, _id id from ziphivespark where state='MA'")
    pro.show()
   /* val host = "jdbc:oracle:thin:@//mforacle.cj3qjsgo9lr7.ap-south-1.rds.amazonaws.com:1521/orcl"
    val prop = new java.util.Properties()
    prop.setProperty("user","ousername")
    prop.setProperty("password","opassword")
    prop.setProperty("driver","oracle.jdbc.OracleDriver")
    pro.write.jdbc(host,"hivetabdhar",prop)*/

    spark.stop()
  }
}