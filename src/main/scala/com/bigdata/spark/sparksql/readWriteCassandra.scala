package bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql._
import org.apache.spark.sql.cassandra._

object readWriteCassandra {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("readWriteCassandra").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("readWriteCassandra").getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val df = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .option("table", "asl").option("keyspace", "january")
      .load() // This Dataset will use a spark.cassandra.input.size of 128
      //df.show()

    val df1 = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .option("table", "nep").option("keyspace", "january")
      .load().withColumnRenamed("name","name1") // This Dataset will use a spark.cassandra.input.size of 128

    //df1.show()
    val res = df1.join(df,df1("name1")===df("name"),"inner").select("id","name","city","email")
    res.show()
    res.write.mode(SaveMode.Append).format("org.apache.spark.sql.cassandra")
      .option("table", "oramysqljoin").option("keyspace", "january")
    spark.stop()
  }
}