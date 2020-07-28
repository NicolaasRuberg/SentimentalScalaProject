name := "Twitter_sentiment_analysis"

version := "0.1"

scalaVersion := "2.11.12"

scalacOptions ++= List("-feature","-deprecation", "-unchecked", "-Xlint")

// https://mvnrepository.com/artifact/org.apache.spark/spark-core

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.5",
//  "org.apache.spark" %% "spark-sql" % "2.4.5",
  "org.apache.spark" %% "spark-mllib" % "2.4.5",
//  "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly(),
  "org.apache.spark" %% "spark-streaming" % "2.4.5",
  "org.twitter4j" % "twitter4j-core" % "4.0.7",
  "org.twitter4j" % "twitter4j-stream" % "4.0.4",
  "org.apache.bahir" %% "spark-streaming-twitter" % "2.3.4"
)

//libraryDependencies ++= Seq(
//  "org.apache.spark" %% "spark-core" % "2.4.5",
//  "org.apache.spark" %% "spark-sql" % "2.4.5",
//  "org.apache.spark" %% "spark-mllib" % "2.4.5",
//  "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly(),
//  "org.apache.spark" %% "spark-streaming" % "2.4.5",
//  "org.twitter4j" % "twitter4j-core" % "4.0.0",
//  "org.twitter4j" % "twitter4j-stream" % "4.0.0",
//  "org.apache.bahir" %% "spark-streaming-twitter" % "2.4.0"
//)
