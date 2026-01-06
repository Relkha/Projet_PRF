ThisBuild / scalaVersion := "2.12.18"
ThisBuild / organization := "com.example"
ThisBuild / version := "0.1.0"

lazy val root = (project in file("."))
  .settings(
    name := "urban-pollution-spark",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.5.1" % "provided",
      "org.apache.spark" %% "spark-mllib" % "3.5.1" % "provided",
      "org.apache.spark" %% "spark-graphx" % "3.5.1" % "provided",
      "org.apache.spark" %% "spark-streaming" % "3.5.1" % "provided"
    )
  )
