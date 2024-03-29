ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.10"

lazy val root = (project in file("."))
  .settings(
    name := "spark-training"
  )


libraryDependencies += "org.apache.spark" %% "spark-core" % "3.3.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.0" % "provided"
