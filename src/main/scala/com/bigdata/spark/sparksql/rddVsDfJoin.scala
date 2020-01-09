package bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql._

object rddVsDfJoin {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("rddVsDfJoin").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("rddVsDfJoin").getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql

    val asl = "C:\\work\\dataset\\asl.csv"
    val nep = "C:\\work\\dataset\\nep.csv"

    //Create RDD
    val aslrdd = sc.textFile(asl)
    val neprdd = sc.textFile(nep)
    val askip = aslrdd.first()
    val nskip = neprdd.first()
    val aproc = aslrdd.filter(x=>x!=askip).map(x=>x.split(",")).map(x=>(x(0),x(1),x(2)))
    val nproc = neprdd.filter(x=>x!=nskip).map(x=>x.split(",")).map(x=>(x(0),x(1),x(2)))
    val akey = aproc.keyBy(x=>x._1) //name
    val nkey = nproc.keyBy(x=>x._1) //name
    val joinRdd = akey.join(nkey)
    //joinRdd.foreach(println)
    val joinrdd1 = akey.join(nkey).map(x=>(x._1,x._2._1._3))
    joinrdd1.foreach(println)

/*    //Create dataframe
    val adf = spark.read.format("csv").option("header","true").option("inferSchema","true").load(asl)
    val ndf = spark.read.format("csv").option("header","true").option("inferSchema","true").load(nep)

    //Join the two dataframes   (inner join) --- when having there is no common name in the tables.
    val pro = adf.join(ndf, adf("name")===ndf("ename"),"inner").drop("ename")

    //when having one common name:
    //val pro1 = adf.join(ndf, $"name","inner")

    // right outer join
    val rightJoinDf = adf.join(ndf, adf("name")===ndf("ename"),"right_outer").drop("ename")
    rightJoinDf.show()

    //broadcast Join :   if you have two tables/dataframes (one has 130cr records and other has 1000 records) than use Broadcast join
    // largedataframe.join(broadcast(smalldataframe), "key")
    */
    spark.stop()
  }
}