name := "handle spark"

organization := "jp.co.ca-adv"

version := "1.0"

description := "sample project of apache-spark"

resolvers ++= Seq(
  "kuromoji repo" at "http://www.atilika.org/nexus/content/repositories/atilika",
  "sbt-plugin-releases-scalasbt" at "http://repo.scala-sbt.org/scalasbt/sbt-plugin-releases/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.2.1",
  "org.apache.spark" %% "spark-mllib"  % "1.2.0"
)
