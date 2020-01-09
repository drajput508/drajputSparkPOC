package bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql._

object complexJsonZomatoData {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("complexJsonZomatoData").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("complexJsonZomatoData").getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql

    val data = "C:\\work\\dataset\\zomato-restaurants-data\\*.json"
    val df = spark.read.format("json").option("inferSchema","true").load(data)
    //df.printSchema()
    df.createOrReplaceTempView("zomatodata")
/*    val query ="select code,message,results_found,results_shown,results_start,status, r.restaurant.R.res_id," +
      " r.restaurant.apikey,r.restaurant.average_cost_for_two, r.restaurant.location.*," +
      "r.restaurant.events_url,r.restaurant.R.res_id, r.* from zomatodata lateral view explode(restaurants) X as r"*/

    val res = spark.sql("select code,message,results_found,status,results_start,results_shown,r.restaurant.R.res_id,r.restaurant.apikey,r.restaurant.average_cost_for_two,r.restaurant.book_url,r.restaurant.cuisines, r.restaurant.currency,r.restaurant.location.*, r.restaurant.offers, r.restaurant.user_rating.*, r.restaurant.zomato_events from zomatodata lateral view explode(restaurants) X as r")
    //For nested array stuct need explode again
    res.createOrReplaceTempView("zomatodata1")
    val res1 = spark.sql("select *, o.offer.*,z.event.*  from zomatodata1 lateral view explode(offers) A as o lateral view explode(zomato_events) B as z").drop("offers","zomato_events","o","z")
    res1.createOrReplaceTempView("zomatodata2")
    val res2 = spark.sql("select *,p.photo.*,rs from zomatodata2 lateral view explode(photos) C as p lateral view explode(restaurant_list) D as rs").drop("photos","p","restaurant_list","restaurants")
    res2.printSchema()
    res2.show()

    spark.stop()
  }
}