package bigdata.spark.sparksql

import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._

object importComplexJsonDataIntoDB {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("importComplexJsonDataIntoDB").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("importComplexJsonDataIntoDB").getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data = "C:\\work\\dataset\\world_bank.json"
    val df = spark.read.format("json").option("inferSchema","true").load(data)
    df.createOrReplaceTempView("worldbank")

    val query = "select distinct _id.`$oid` as oid,approvalfy," +
      "board_approval_month," +
      "boardapprovaldate," +
      "borrower," +
      "closingdate," +
      "country_namecode," +
      "countrycode," +
      "countryname," +
      "countryshortname," +
      "docty,envassesmentcategorycode,grantamt," +
      "ibrdcommamt,id,idacommamt,impagency,lendinginstr,lendinginstrtype," +
      "lendprojectcost," +
      "mp.Name as mpname, mp.Percent as mppercent," +
      "mj.code as mjcode, mj.name as mjname," +
      "mjt.code as mjtcode, mjt.name as mjtname," +
      "mjthemecode,prodline,prodlinetext,productlinetype," +
      "project_name," +
      "pd.DocDate,pd.DocType,pd.DocTypeDesc,pd.DocURL,pd.EntityID," +
      "projectfinancialtype,projectstatusdisplay,regionname," +
      "s.Name as sectorname," +
      "sector1.Name as s1name,sector1.Percent as s1percent," +
      "sector2.Name as s2name,sector2.Percent as s2percent," +
      "sector3.Name as s3name,sector3.Percent as s3percent," +
      "sector4.Name as s4name,sector4.Percent as s4percent," +
      "snc.code as snccode,snc.name as sncname," +
      "sectorcode,source,status,supplementprojectflg," +
      "theme1.name as theme1name,theme1.Percent as theme1Per," +
      "thn.code as thenamecode,thn.name as themename," +
      "themecode,totalamt,totalcommamt,url " +
      "from worldbank  " +
      "lateral view explode(majorsector_percent) tmp as mp " +
      "lateral view explode(mjsector_namecode) tmp1 as mj " +
      "lateral view explode(mjtheme_namecode) tmp2 as mjt " +
      "lateral view explode(projectdocs) tmp3 as pd " +
      "lateral view explode(sector) tmp4 as s " +
      "lateral view explode(sector_namecode) tmp5 as snc " +
      "lateral view explode(theme_namecode) tmp6 as thn"

     val res = spark.sql(query)
     //res.show()

    // Oracle DB connection properties
    val host = "jdbc:oracle:thin:@//mforacle.cj3qjsgo9lr7.ap-south-1.rds.amazonaws.com:1521/orcl"
    val prop = new Properties()
    prop.setProperty("user","ousername")
    prop.setProperty("password","opassword")
    prop.setProperty("driver","oracle.jdbc.OracleDriver")

    res.write.jdbc(host,"worldbank",prop)
    val result = spark.read.jdbc(host,"worldbank",prop)
    result.show(4,false)
    spark.stop()
  }
}