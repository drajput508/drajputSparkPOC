package bigdata.spark.sparksql

import org.apache.spark.sql.{SparkSession, _}

object importAllTable {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("importAllTable").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("importAllTable").enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext

    //val tabs = Array("EMP","DEPT")

    val host = "jdbc:oracle:thin:@//mforacle.cj3qjsgo9lr7.ap-south-1.rds.amazonaws.com:1521/orcl"
    val prop = new java.util.Properties()
    prop.setProperty("user","ousername")
    prop.setProperty("password","opassword")
    prop.setProperty("driver","oracle.jdbc.OracleDriver")
    val query = "(select table_name from all_tables where TABLESPACE_NAME='USERS') tmp"
    // All query will get all the tables from the Oracle DB
    val allTableDf = spark.read.jdbc(host,query,prop)
    // Convert alldf dataframe into array
    val tabs = allTableDf.select("TABLE_NAME").rdd.map(x=>x(0)).collect().toArray
      tabs.foreach{ x=>
      val tab = x.toString()
      val res = spark.read.jdbc(host,tab,prop)

        // Import the all table into the hive
      //res.write.mode(SaveMode.Append).format("hive").saveAsTable(tab)
         val path = args(0)
       // Save the tables data as CSV
        res.write.mode(SaveMode.Append).format("csv").option("header","true").save(s"$path/$tab")
      }


    spark.stop()
  }
}