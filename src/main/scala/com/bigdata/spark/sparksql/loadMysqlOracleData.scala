package bigdata.spark.sparksql

import java.util.Properties

import jdk.nashorn.internal.runtime.Property
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._

object loadMysqlOracleData {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("loadMysqlOracleData").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("loadMysqlOracleData").getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val host = "jdbc:oracle:thin:@//mforacle.cj3qjsgo9lr7.ap-south-1.rds.amazonaws.com:1521/orcl"

    val prop = new java.util.Properties()
    prop.setProperty("user","ousername")
    prop.setProperty("password","opassword")
    prop.setProperty("driver","oracle.jdbc.OracleDriver")

    val empdf = spark.read.jdbc(host,"EMP",prop)

    val myhost = "jdbc:mysql://mysql.conbyj3qndaj.ap-south-1.rds.amazonaws.com:3306/venutasks"
    val mysqlProp = new java.util.Properties()
    mysqlProp.setProperty("user","ousername")
    mysqlProp.setProperty("password","opassword")
    mysqlProp.setProperty("driver","com.mysql.jdbc.Driver")

    val deptdf = spark.read.jdbc(myhost,"dept",mysqlProp)

    empdf.createOrReplaceTempView("T1")
    deptdf.createOrReplaceTempView("T2")

    val join = spark.sql("select T2.loc, T1.* from T1 join T2 on T1.deptno=T2.deptno")
    join.show()

    spark.stop()
  }
}

