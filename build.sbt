ThisBuild / version := "1.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.16"

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

lazy val root = (project in file("."))
  .settings(
    name := "mario-inc-exercise",
    idePackagePrefix := Some("it.scarpenti.marioinc"),
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.2.1",
  "org.apache.spark" %% "spark-sql" % "3.2.1",
  "org.apache.spark" %% "spark-hive" % "3.2.1",
  "io.delta" %% "delta-core" % "1.2.1",
  "org.scalatest" %% "scalatest" % "3.2.12" % "test",
  "org.scalacheck" %% "scalacheck" % "1.16.0" % "test",
  "com.typesafe" % "config" % "1.4.2",
  "org.backuity.clist" %% "clist-core" % "3.5.1",
  "org.backuity.clist" %% "clist-macros" % "3.5.1" % "provided",
  "org.apache.logging.log4j" %% "log4j-api-scala" % "12.0",
  "org.apache.logging.log4j" % "log4j-core" % "2.17.2" % Runtime,
)

