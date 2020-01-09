package bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import org.apache.spark.sql._

object sparkSqlQuries {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("sparkSqlQuries").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("sparkSqlQuries").getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data = "C:\\work\\dataset\\10000 Records.csv"
    val regx = "[^\\p{L}\\p{Nd}]+"
    val df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)

    // Replace special characters from header
    val columns = df.columns.map(x=>x.replaceAll(regx,""))

    // Convert RDD to Dataframe
    val ndf = df.toDF(columns:_*)
   // Group by Email (yahoo or gmail or hotmail....)
     val groupByEmailDf = ndf.withColumn("_tmp", split($"Email", "\\@")).select($"_tmp".getItem(1).as("col2")).groupBy("col2").count()
     groupByEmailDf.show()

    // Employee with max age (>50)
    val maxAgeDf = ndf.where($"AgeinYrs">50)
    maxAgeDf.show()

    // Maximum years in the company
    val maxYearOfJoing = ndf.sort(asc("DateofJoining")).select("EmpId","FirstName","LastName","DateofJoining").limit(1)
    maxYearOfJoing.show()

    //Employee Join the company group by year
    val empJoinByYear = ndf.groupBy("YearofJoining").count().sort(desc("count"))
    empJoinByYear.show()

    //10 Maximum Salary holder
    val maxSalaryHolder = ndf.select("EmpId","FirstName","LastName","DateofJoining","Salary").sort(desc("salary")).limit(10)
    maxSalaryHolder.show()
    maxSalaryHolder.write.mode("overwrite").format("csv").option("delimiter","\\t").save("C:\\work\\dataset\\output\\maxsalarHolder")

    // Anyone brothers or sisters? /Having same mother/father ? (Father's Name,Mother's Name)
    ndf.createOrReplaceTempView("emprecords")

    val brothersAndSistersDf = spark.sql("select a.FirstName, b.FirstName from emprecords a join emprecords b on a.FathersName = b.FathersName and a.MothersName = b.MothersName where a.Gender='M' and b.Gender='F'")
    brothersAndSistersDf.show()

    spark.stop()
  }
}