package bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql._

object importOracleTablesIntoHive {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("importOracleTablesIntoHive").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("importOracleTablesIntoHive").getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val host = "jdbc:oracle:thin:@//mforacle.cj3qjsgo9lr7.ap-south-1.rds.amazonaws.com:1521/orcl"
    val prop = new java.util.Properties()
    prop.setProperty("user","ousername")
    prop.setProperty("password","opassword")
    prop.setProperty("driver","oracle.jdbc.OracleDriver")
    val query = "(select table_name from all_tables where TABLESPACE_NAME='USERS') tmp"
    // Above query will get all the tables from the Oracle DB
    val allTableDf = spark.read.jdbc(host,query,prop)
    allTableDf.show()

/*    val tabs = allTableDf.select("TABLE_NAME").rdd.map(x=>x(0)).collect().toArray    // Convert allTableDf dataframe into array
    tabs.foreach{ x=>
      val tab = x.toString()
      val res = spark.read.jdbc(host,tab,prop)*/

      // Import the all table into the hive
      //res.write.mode(SaveMode.Append).format("hive").saveAsTable(tab)
    spark.stop()
  }
}