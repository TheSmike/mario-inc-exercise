package it.scarpenti.marioinc
package spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkSessionFactory {

  def getSession(profile: String): SparkSession = {
    setLog()

    val builder =
      if (profile == "local") localBuilder()
      else genericBuilder()

    builder
      .appName("Mario Inc. Assignment")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.session.timeZone", "UTC")
      .enableHiveSupport()
      .getOrCreate()
  }

  private def localBuilder() = SparkSession.builder().master("local[1]")

  private def genericBuilder() = SparkSession.builder()

  //TODO These settings should be moved into a log4j config files
  private def setLog(): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("it.scarpenti").setLevel(Level.DEBUG)
  }

}
