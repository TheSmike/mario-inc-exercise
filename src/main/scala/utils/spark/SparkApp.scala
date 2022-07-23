package it.scarpenti.marioinc
package utils.spark

import config.{AppConfig, ConfigReader}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.backuity.clist.Cli

import java.util.TimeZone


abstract class SparkApp[Context <: AbstractContext](implicit ev: Manifest[Context]) {

  val config: AppConfig = new ConfigReader().read()
  val logger: Logger = Logger.getLogger(this.getClass)
  val session: SparkSession = createSession

  def main(implicit args: Array[String]): Unit = {
    val context = init()
    parseArgs(args, context)

    logger.info("Start SparkApp...")

    logger.info(s"input args are: ${args.toList}")
    logger.info(s"Configs are: $config")

    logger.info("START PIPELINE " + this.getClass.getSimpleName)
    run(context)

    logger.info("END SparkApp")
  }

  def init(): Context

  def run(context: Context) : Unit


  //private methods

  private def parseArgs(args: Array[String], context: Context) = {
    Cli.parse(args).withCommand(context) { config =>
      println(config.description)
    }
  }

  //TODO: Parametrize session and its AppName
  private def createSession = {

    val session = SparkSession.builder()
      .master("local[1]")
      .appName("Mario Inc. Assignment")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .enableHiveSupport()
      .getOrCreate()

    setLog()
    //FIXME (improve me): This setting is necessary due to a problem with the generated column of the device_data table,
    // we can alternatively use the right conversion function (with formatting string) in the generated column.
    // (in fact I think it's even better)
    session.conf.set("spark.sql.session.timeZone", "UTC")
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

    session
  }

  //TODO These settings should be moved into a log4j config files
  private def setLog(): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("it.scarpenti").setLevel(Level.DEBUG)
  }
}
