package bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object importZomatoDataToDB {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("importZomatoDataToDB").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("importZomatoDataToDB").getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._

    // Process Json data via Dataframes

    val zomatodata = "C:\\work\\dataset\\zomato-restaurants-data\\file1.json"
    val zomatodf = spark.read.format("json").option("inferSchema", "true").load(zomatodata)

    zomatodf.printSchema()

    /*    root
        |-- code: long (nullable = true)
        |-- message: string (nullable = true)
        |-- restaurants: array (nullable = true)
        |    |-- element: struct (containsNull = true)
        |    |    |-- restaurant: struct (nullable = true)
        |    |    |    |-- R: struct (nullable = true)
        |    |    |    |    |-- res_id: long (nullable = true)
        |    |    |    |-- apikey: string (nullable = true)
        |    |    |    |-- average_cost_for_two: long (nullable = true)
        |    |    |    |-- book_url: string (nullable = true)
        |    |    |    |-- cuisines: string (nullable = true)
        |    |    |    |-- currency: string (nullable = true)
        |    |    |    |-- deeplink: string (nullable = true)
        |    |    |    |-- establishment_types: array (nullable = true)
        |    |    |    |    |-- element: string (containsNull = true)
        |    |    |    |-- events_url: string (nullable = true)*/

    // Use below functions when Json contain the Array:struct:Array type schema
    val restaurantsDF1 = zomatodf.select(explode($"restaurants")).toDF("restaurants").select("restaurants.restaurant.apikey", "restaurants.restaurant.id", "restaurants.restaurant.name", "restaurants.restaurant.url")
    restaurantsDF1.show()

    // join two DataFrames
    val temprestaurantsDF1 = restaurantsDF1.withColumn("id", monotonically_increasing_id())
    //monotonically_increasing_id method to generate incremental numbers.
    // However the numbers won’t be consecutive if the dataframe has more than 1 partition

    val restaurantsDF2 = zomatodf.select("*").drop("restaurants")
    val temprestaurantsDF2 = restaurantsDF2.withColumn("id", monotonically_increasing_id())
    temprestaurantsDF1.join(temprestaurantsDF2, "id").drop("id").show()

/*
    //Process Json Data via Spark SQL
    zomatodf.printSchema()
    zomatodf.createOrReplaceTempView("zomatotab")
    spark.sql("select restaurants[0].restaurant.average_cost_for_two as cost_per_couple from zomatotab").show() //.collect().foreach(println)
    val res = spark.sql("select restaurants[0].restaurant.name as name, restaurants[0].restaurant.average_cost_for_two as price from zomatotab where restaurants[0].restaurant.average_cost_for_two >=1600 ") //.collect().foreach(println)
     val res = spark.sql("select * from zomatotab where restaurants[0].restaurant.average_cost_for_two <=1600")
*/

    /*
    // Import the json data into the Oracle DB
        val host = "jdbc:oracle:thin:@//mforacle.cj3qjsgo9lr7.ap-south-1.rds.amazonaws.com:1521/orcl"
        val prop = new java.util.Properties()
        prop.setProperty("user","ousername")
        prop.setProperty("password","opassword")
        prop.setProperty("driver","oracle.jdbc.OracleDriver")

        res.write.mode(SaveMode.Overwrite).jdbc(host,"ZomatoData",prop)
    */
    spark.stop()
  }
}