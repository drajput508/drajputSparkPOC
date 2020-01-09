package com.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession

object Oscars {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("Oscars").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("Oscars").getOrCreate()
    val sc = spark.sparkContext
    val data = "C:\\work\\Projects\\Oscars\\Oscars.txt"
    val regx = "[^\\p{L}\\p{Nd}]+"
    val df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter", "\t").load(data)
    val cleanHeaderName = df.columns.map(x => x.replaceAll(regx, ""))
    //toDF() used to (1) convert RDD to df. (2) to rename the columns
    val newDF = df.toDF(cleanHeaderName: _*)
    newDF.createOrReplaceTempView("tab")
    // Columns:  unitid|          birthplace|dateofbirth|raceethnicity|yearofaward|    award|   movie|  person|
    //Check DOB quality. Note that length varies based on month name
    spark.sql("select distinct(length(dateofbirth))  from tab").show()

    //Look at the value with unexpected length 4.
    spark.sql("select dateofbirth from tab where length(dateofbirth) = 4").show()
    // output
    /*  +-----------+
      |dateofbirth|
      +-----------+
      |       1972|
        +-----------+*/

    //This is an invalid date. We can either drop this record or give some meaningful value like 01-01-1972
    //UDF to clean date
    //This function takes 2 digit year and makes it 4 digit
    // Any exception returns an empty string
    def fnCleanDate(s: String): String = {
      var cleanDate = ""
      val dateArray: Array[String] = s.split("-")
      try { //adjust the yr
        var yr = dateArray(2).toInt
        if (yr < 100) {
          yr = yr + 1900
        }
        // Make it 4 digit
        cleanDate = "%2d-%s-%4d".format(dateArray(0).toInt, dateArray(1), yr)
      } catch {
        case e: Exception => None
      }
      cleanDate
    }

    //UDF to clean birthplace
    // Data explorartion showed that
    // A. Country is omitted for USA
    // B. New York City does not have State code as well
    //This function appends country as USA if
    // A. the string contains New York City  (OR)
    // B. if the last component is of length 2 (eg CA, MA)

    def fnCleanBirthPlace(s: String): String = {
      var cleanBirthPlace = ""
      var strArray: Array[String] = s.split(" ")
      if (s == "New York City")
        strArray = strArray ++ Array("USA")
      //Append country if last element length is 2
      else if (strArray(strArray.length - 1).length == 2)
        strArray = strArray ++ Array("USA")

      cleanBirthPlace = cleanBirthPlace.mkString(" ")
      cleanBirthPlace
    }
    //Register UDFs
    spark.udf.register("fnCleanDate", fnCleanDate(_: String))
    spark.udf.register("fnCleanBirthPlace", fnCleanBirthPlace(_: String))

    //Perform the following cleanup operations:
    // 1. Call udfs fncleanData and fncleanBirthplace to fix birthplace and country
    // 2. Subtract birth year from award_year to get age at the time of receiving the award
    // 3. Retain race and award as they are
    //Note 1: Use 3 double quotes for multiline string in scala
    //Note 2: Note that cleaned_df is declared as var, because we intend to reuse variable name
    //Note 3: substring_index returns part of first argument that starts with second argument.
    // -1 indicates first occurrence from right side

    var cleaned_df = spark.sql(
      """
        |SELECT fnCleanDate(dateofbirth) dob,
        |fnCleanBirthPlace(birthplace) birthplace,
        |substring_index(fnCleanBirthPlace(birthplace),' ',-1) country,
        |(yearofaward - substring_index(fncleanDate(dateofbirth),'-',-1)) age,
        |raceethnicity , award from tab""".stripMargin)

    //Missing value treatment
    // Drop rows with incomplete information
    cleaned_df = cleaned_df.na.drop
    cleaned_df.show()
    spark.stop()
  }
}