package it.scarpenti.marioinc
package pipeline.data

import utils.spark.SparkApp

import org.apache.spark.sql.functions.{date_add, datediff, to_date}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object DeviceDataPipeline extends SparkApp[DeviceDataContext] {

  override def init(args: Array[String]): DeviceDataContext = new DeviceDataContext(args)

  override def run(session: SparkSession, context: DeviceDataContext): Unit = {
    val bronze = session.read.format("delta").table(context.rawDataTableName)
    logger.debug("schema is ==> " + bronze.schema)

    val filtered = cleanData(context.receivedDate, bronze)

    filtered
      .write
      .format("delta")
      .partitionBy("received")
      .mode(SaveMode.Overwrite)
      .option("replaceWhere", s"received = '${context.receivedDate}'")
      .save(context.outputPath)

  }

  //TODO we can refactor this method again to move the first filter to another place, this could help us isolate tests more
  def cleanData(receivedDate: String, bronze: DataFrame) = {
    bronze
      .filter(bronze("received") === receivedDate)
      .filter(datediff(bronze("received"), to_date(bronze("timestamp"))) <= 1) //TODO is it useful to save the number of discarded records?
      .dropDuplicates("device", "timestamp")
  }
}

