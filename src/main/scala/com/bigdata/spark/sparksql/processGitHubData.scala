package bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql._

object processGitHubData {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("processGitHubData").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("processGitHubData").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    import spark.sql
    val gitHubData = "C:\\work\\dataset\\2019-03-01-23.json"

    val gitHubDf = spark.read.format("json").option("inferSchema","true").load(gitHubData)
    gitHubDf.printSchema()
    gitHubDf.createOrReplaceTempView("tab")

    spark.sql("select payload.commits.author.name from tab limit 10").collect().foreach(println)
    //res.show()

    //gitHubDf.filter("payload.comment.id IS NOT NULL").select("payload.*").show()

    spark.stop()
  }
}