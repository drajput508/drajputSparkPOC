package bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import org.apache.spark.sql._

object importJsonDataToOracle {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("importJsonDataToOracle").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("importJsonDataToOracle").getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql

    val zipdata = "C:\\work\\dataset\\zips1.json"
    val zipdf = spark.read.format("json").option("inferSchema","true").load(zipdata)
    //Cleaning json data by spark sql
    zipdf.createOrReplaceTempView("tab")
    val res = spark.sql("select _id id, city,loc[0] lang, loc[1] lati,pop,state from tab")
    res.show(5,false)

    //Oracle DB connection properties
    val host = "jdbc:oracle:thin:@//bigdataoracle.cfpbfnzmymes.ap-south-1.rds.amazonaws.com:1521/ORCL"
    //val host = "jdbc:oracle:thin:@//mforacle.cj3qjsgo9lr7.ap-south-1.rds.amazonaws.com:1521/orcl"
    val prop = new java.util.Properties()
    prop.setProperty("user","ousername")
    prop.setProperty("password","opassword")
    prop.setProperty("driver","oracle.jdbc.OracleDriver")

    /*
    after running below code i was getting the below error message
    df.write.jdbc(host,"Dharmendra_zip_task",prop)
   -->  Exception in thread "main" java.lang.IllegalArgumentException: Can't get JDBC type for array<string>
     needed to remove the columns with array type before writing to the databases.
    //https://stackoverflow.com/questions/50201887/java-lang-illegalargumentexception-cant-get-jdbc-type-for-arraystring
    we can create a string with comma separated for the column type array as
    */

    zipdf.withColumn("loc",concat_ws(",",$"loc")).write.mode(SaveMode.Overwrite).jdbc(host,"Dharmendra_zip_task",prop)

    //concat_ws creates a string the values in an array with delimiter provided.
    spark.stop()
  }
}

