package bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql._

object sparkCassandraProject {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("sparkCassandraProject").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("sparkCassandraProject").getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val ohost = "jdbc:oracle:thin:@//mforacle.cj3qjsgo9lr7.ap-south-1.rds.amazonaws.com:1521/orcl"
    val prop = new java.util.Properties()
    prop.setProperty("user","ousername")
    prop.setProperty("password","opassword")
    prop.setProperty("driver","oracle.jdbc.OracleDriver")
    val empdf = spark.read.jdbc(ohost,"emp",prop)

    val myhost = "jdbc:mysql://mysql.conbyj3qndaj.ap-south-1.rds.amazonaws.com:3306/venutasks"
    val mysqlProp = new java.util.Properties()
    mysqlProp.setProperty("user","ousername")
    mysqlProp.setProperty("password","opassword")
    mysqlProp.setProperty("driver","com.mysql.jdbc.Driver")
    val deptdf = spark.read.jdbc(myhost,"dept",mysqlProp)

    empdf.createOrReplaceTempView("empdb")
    deptdf.createOrReplaceTempView("deptdb")

    val join1 = spark.sql("SELECT d.deptno,d.dname,d.loc,e.job,e.sal,e.ename FROM empdb e join deptdb d on e.deptno=d.deptno")
    join1.write.mode(SaveMode.Append).format("org.apache.spark.sql.cassandra")
      .option("table", "oramysqljoin").option("keyspace", "january").save()
  /*  val createDDL = """create table oramysqljoin (deptno int, dname varchar, loc varchar, job varchar,sal int, ename int PRIMARY KEY);
     USING org.apache.spark.sql.cassandra
     OPTIONS (
     table "oramysqljoin",
     keyspace "january",
     cluster "Test Cluster",
     pushdown "true")"""
    spark.sql(createDDL)
      join1.show()*/

   // join1.write.mode(SaveMode.Append).format("org.apache.spark.sql.cassandra").option("table", "oramysqljoin").option("keyspace", "january").save()

    spark.stop()
  }
}