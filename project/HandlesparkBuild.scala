import sbt._
import sbt.Keys._

object HandlesparkBuild extends Build {

  lazy val handlespark = Project(
    id = "handle-spark",
    base = file("."),
    settings = Project.defaultSettings ++ Seq(
      name := "handle-spark",
      organization := "org.example",
      version := "0.1-SNAPSHOT",
      scalaVersion := "2.11.4"
      // add other settings here
    )
  )
}
