name := "SparkPOC"

version := "0.1"

scalaVersion := "2.11.0"

//https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies +="org.apache.spark" %% "spark-sql" % "2.4.4"
//https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies +="org.apache.spark" %% "spark-core" % "2.4.4"

// https://mvnrepository.com/artifact/org.scala-lang.modules/scala-xml
libraryDependencies += "org.scala-lang.modules" %% "scala-xml" % "1.1.0"

// https://mvnrepository.com/artifact/com.twitter/jsr166e
libraryDependencies += "com.twitter" % "jsr166e" % "1.1.0"


libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.2"


// https://mvnrepository.com/artifact/org.apache.phoenix/phoenix-spark
libraryDependencies += "org.apache.phoenix" % "phoenix-spark" % "4.14.3-HBase-1.4"

// https://mvnrepository.com/artifact/org.apache.kafka/kafka
libraryDependencies += "org.apache.kafka" %% "kafka" % "2.4.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.4" % "provided"

// https://mvnrepository.com/artifact/org.apache.bahir/spark-streaming-twitter
libraryDependencies += "org.apache.bahir" %% "spark-streaming-twitter" % "2.4.0"

// https://mvnrepository.com/artifact/org.twitter4j/twitter4j-stream
libraryDependencies += "org.twitter4j" % "twitter4j-stream" % "4.0.4"





