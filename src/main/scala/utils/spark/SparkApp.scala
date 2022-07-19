package it.scarpenti.marioinc
package utils.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import java.util.TimeZone


abstract class SparkApp[Context] {

  val logger = Logger.getLogger(this.getClass)

  def logContext(context: Context): Unit = {
    logger.info("Context is composed by:")
    for (v <- context.getClass.getDeclaredFields) {
      v.setAccessible(true)
      logger.info("Field: " + v.getName() + " => " + v.get(context))
    }
  }

  def main(implicit args: Array[String]): Unit = {
    logger.info("INIT CONTEXT")
    logger.info(s"input args are: ${args.toList}")
    val context = init(args)
    logContext(context)

    logger.info("START PIPELINE " + this.getClass.getSimpleName)
    run(session, context)
    logger.info("END PIPELINE")
  }

  def init(args: Array[String]): Context

  def run(session: SparkSession, context: Context)


  //TODO: Parametrize session and its AppName
  val session =
    createSession

  private def createSession = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("it.scarpenti").setLevel(Level.DEBUG)
    val session = SparkSession.builder()
      .master("local[1]")
      .appName("Mario Inc. Assignment")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .enableHiveSupport()
      .getOrCreate()

    //FIXME (improve me): This setting is necessary due to a problem with the generated column of the device_data table,
    // we can alternatively use the right conversion function (with formatting string) in the generated column.
    // (in fact I think it's even better)
    session.conf.set("spark.sql.session.timeZone", "UTC")
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

    session
  }
}
