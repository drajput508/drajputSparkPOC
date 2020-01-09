package com.bigdata.spark.sparkstreaming

import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.sql._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe


object consumer {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("streamingTest").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[2]").appName("streamingTest").getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))

    import spark.implicits._
    import spark.sql


    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("test")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val lines = stream.map(record => record.value)
    lines.foreachRDD { rdd =>
      val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      val df = rdd.map(x=>x.split(",")).map(x=>(x(0),x(1),x(2))).toDF("name","age","city")
      df.createOrReplaceTempView("tab")
      val res = spark.sql("select * from tab")

      // Save data into oracle DB
     /* val host = "jdbc:oracle:thin:@//mforacle.cj3qjsgo9lr7.ap-south-1.rds.amazonaws.com:1521/orcl"
      val prop = new Properties()
      prop.setProperty("user","ousername")
      prop.setProperty("password","opassword")
      prop.setProperty("driver","oracle.jdbc.OracleDriver")

      res.write.mode(SaveMode.Append).jdbc(host,"livedata",prop)*/
      res.show()
    }



    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate

    spark.stop()
  }
}