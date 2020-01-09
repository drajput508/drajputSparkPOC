package com.bigdata.spark.sparkstreaming

import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object importLiveDataIntoOracle {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("streamingTest").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("streamingTest").getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    // create a Dstream taht will connect to hostname:port, like localhost:9999
    val lines = ssc.socketTextStream("drajput1c.mylabserver.com",30080)
    import spark.implicits._
    import spark.sql
    //lines.print()
    // Get the emp information belongs to hyderabaad and save it in next file
    //RDD
    /*   val res = lines.map(x=>x.split(",")).map(x=>(x(0),x(1),x(2))).filter(x=>x._3 == "hyd")
       res.saveAsTextFiles("C:\\work\\output\\hydinfo")
       res.print()
       */

    // Using DF
    lines.foreachRDD { rdd =>
      val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      val df = rdd.map(x=>x.split(",")).map(x=>(x(0),x(1),x(2))).toDF("name","age","city")
      df.createOrReplaceTempView("tab")
      val res = spark.sql("select * from tab where city='hyd'")

      // Save data into oracle DB
      val host = "jdbc:oracle:thin:@//mforacle.cj3qjsgo9lr7.ap-south-1.rds.amazonaws.com:1521/orcl"
      val prop = new Properties()
      prop.setProperty("user","ousername")
      prop.setProperty("password","opassword")
      prop.setProperty("driver","oracle.jdbc.OracleDriver")

      res.write.mode(SaveMode.Append).jdbc(host,"livedata",prop)


      //res.show()
    }
    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
  }
}