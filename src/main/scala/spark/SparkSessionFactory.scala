package it.scarpenti.marioinc
package spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkSessionFactory {

  private var profile: String = null
  private lazy val session = createSession(profile)


  def getSession(profile: String): SparkSession = {
    if (SparkSessionFactory.profile == null)
      SparkSessionFactory.profile = profile
    else if (SparkSessionFactory.profile != profile)
      throw new Exception(s"Session already initialized with profile ${SparkSessionFactory.profile}")

    session
  }

  private def createSession(profile: String) = {
    setLog()

    val builder =
      if (profile == "local") localBuilder()
      else genericBuilder()

    builder
      .appName("Mario Inc. Assignment")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.sql.datetime.java8API.enabled", "true")
      .enableHiveSupport()
      .getOrCreate()
  }

  private def localBuilder() =
    SparkSession.builder()
      .master("local[1]")
      .config("spark.sql.warehouse.dir", "/tmp/marioinc/spark-warehouse")

  private def genericBuilder() = SparkSession.builder()

  //TODO These settings should be moved into a log4j config files
  private def setLog(): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("it.scarpenti").setLevel(Level.DEBUG)
  }

}
