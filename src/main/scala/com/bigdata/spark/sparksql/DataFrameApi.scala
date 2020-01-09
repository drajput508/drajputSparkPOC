package bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql._

object DataFrameApi {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("DataFrameApi").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("DataFrameApi").getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql

    /*
   val sqlContext = new SQLContext(sc)
   val df = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true") // Use first line of all files as header
    .option("inferSchema", "true") // Automatically infer data types
    .load("cars.csv")
     */
   //val data = "C:\\work\\dataset\\bank-full.csv"
   //val output = "C:\\work\\dataset\\new\\bank-full"

    val data = args(0)
    val output = args(1)

    val df = spark.read.format("csv").option("header","true").option("inferSchema","true").option("delimiter",";").load(data)
    //df.show()
    //df.printSchema()

    // Convert dataframe into table view:
    df.createOrReplaceTempView("bankdata")
    // Query the table
    val res = spark.sql("select * from bankdata where marital = 'married' and loan!='no' and balance > '50000'")
    //res.show()

    res.write.format("csv").option("header","true").save(output)
    spark.stop()
  }
}