package it.scarpenti.marioinc

import config.{AppConfig, ConfigReader}
import spark.SparkSessionFactory

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.backuity.clist.Cli


abstract class SparkApp[Context <: AbstractContext](implicit ev: Manifest[Context]) {

  lazy val config: AppConfig = new ConfigReader(profile).read()
  val logger: Logger = Logger.getLogger(this.getClass)
  lazy val session: SparkSession = SparkSessionFactory.getSession(profile)
  private var profile: String = null

  def main(implicit args: Array[String]): Unit = {
    val context = init()
    parseArgs(args, context)
    profile = context.profile
    logger.info(s"Spark session UI url is ${session.sparkContext.uiWebUrl}")

    logger.info("Start SparkApp...")

    logger.info(s"input args are: ${args.toList}")
    logger.info(s"Configs are: $config")

    logger.info("START PIPELINE " + this.getClass.getSimpleName)
    run(context)

    logger.info("END SparkApp")
  }

  private def parseArgs(args: Array[String], context: Context) = {
    Cli.parse(args).withCommand(context) { config =>
      println(config.description)
    }
  }

  def init(): Context

  def run(context: Context): Unit

}
