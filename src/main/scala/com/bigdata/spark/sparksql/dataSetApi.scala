package bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql._

object dataSetApi {
  case class uscc(first_name:String,last_name:String,company_name:String,address:String,city:String,county:String,state:String,zip:String,phone1:String,phone2:String,email:String,web:String)

  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("dataSetApi").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("dataSetApi").getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql

    val data = "file:///C:\\work\\dataset\\us-500.csv"

    val df = spark.read.format("csv").option("header","true").load(data)   // Dataframe

    val ds = spark.read.format("csv").option("header","true").load(data).as[uscc] // DataSet

    spark.stop()
  }
}