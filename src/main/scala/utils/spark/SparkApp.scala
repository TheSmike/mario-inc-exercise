package it.scarpenti.marioinc
package utils.spark

import org.apache.spark.sql.SparkSession
import org.apache.logging.log4j.scala.Logging


abstract class SparkApp[Context] extends Logging {

  def logContext(context: Context): Unit = {
    logger.info("Context is composed by:")
    for(v <- context.getClass.getDeclaredFields){
      v.setAccessible(true)
      logger.info("Field: " + v.getName() + " => " + v.get(context))
    }
  }

  def main(implicit args: Array[String]): Unit = {
    logger.info("INIT CONTEXT")
    logger.info(s"input args are: ${args.toList}")
    val context = init(args)
    logContext(context)

    logger.info("START PIPELINE " + this.getClass.getSimpleName )
    run(session, context)
    logger.info("END PIPELINE")
  }

  def init(args: Array[String]) : Context

  def run(session: SparkSession, context : Context)


 //TODO: Parametrize session and its name
  val session = SparkSession.builder()
    .master("local[1]")
    .appName("Mario Inc. Assignment")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()

}
