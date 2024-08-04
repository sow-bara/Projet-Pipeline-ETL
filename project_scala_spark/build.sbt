import sbtassembly.AssemblyPlugin.autoImport._
import sbtassembly.MergeStrategy
import sbt._

ThisBuild / version := "0.1.0"
ThisBuild / scalaVersion := "2.12.12"

lazy val sparkVersion = sys.env.getOrElse("SPARK_VERSION", "2.4.5")

lazy val root = (project in file("."))
  .settings(
    name := "project_scala_spark",
    mainClass in assembly := Some("scala.Main"),
    assemblyJarName in assembly := "project_scala_spark.jar",
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
      case x => MergeStrategy.first
    },
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5"
    )
  )
