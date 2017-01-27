name := "handle spark"

scalaVersion := "2.11.5"

organization := "com.example"

version := "1.0"

description := "sample project of apache-spark"

resolvers ++= Seq(
  "kuromoji repo" at "http://www.atilika.org/nexus/content/repositories/atilika",
  "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases/",
  "Sonatype snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
  "sbt-plugin-releases-scalasbt" at "http://repo.scala-sbt.org/scalasbt/sbt-plugin-releases/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.2.1",
  "org.apache.spark" %% "spark-mllib"  % "1.2.1",
  "org.atilika.kuromoji" % "kuromoji" % "0.7.7",
  "org.apache.spark" %% "spark-streaming-twitter" % "1.2.1",
  "org.apache.spark" %% "spark-streaming" % "1.2.1",
  "org.elasticsearch" % "elasticsearch-spark" % "2.1.0.BUILD-SNAPSHOT",
  "org.twitter4j" % "twitter4j-stream" % "3.0.3"
)
