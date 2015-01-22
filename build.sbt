name := "handle spark"

scalaVersion := "2.11.5"

organization := "jp.co.ca-adv"

version := "1.0"

description := "sample project of apache-spark"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.2.0",
  "org.apache.spark" %% "spark-mllib"  % "1.2.0"
)
