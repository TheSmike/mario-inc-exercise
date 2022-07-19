package it.scarpenti.marioinc
package pipeline.data

import utils.spark.SparkApp

import org.apache.spark.sql.functions.{datediff, to_date, to_timestamp}
import org.apache.spark.sql._

object DeviceDataPipeline extends SparkApp[DeviceDataContext] {

  override def init(args: Array[String]): DeviceDataContext = new DeviceDataContext(args)

  override def run(session: SparkSession, context: DeviceDataContext): Unit = {
    val rawData = session.read.format("delta").table(context.rawDataTableName)
    logger.debug("schema is ==> " + rawData.schema)

    val filtered = cleanData(context.receivedDate, rawData)

    val projected = projectData(filtered)

    projected
      .write
      .format("delta")
      .mode(SaveMode.Overwrite)
      .option("replaceWhere", s"received_date = '${context.receivedDate}'")
      .saveAsTable(context.dataTableName)

  }

  //TODO we can refactor this method again to move the first filter to another place, this could help us isolate tests more
  def cleanData(receivedDate: String, bronze: DataFrame) = {
    bronze
      .filter(bronze("received") === receivedDate)
      .filter(datediff(bronze("received"), to_date(bronze("timestamp"))) <= 1) //TODO Could it be useful to save the number of discarded records?
      .dropDuplicates("device", "timestamp")
  }

  def projectData(filtered: Dataset[Row]) = {

    filtered
      .withColumnRenamed("received", "received_date")
      .withColumn("event_timestamp", to_timestamp(filtered("timestamp"), "y-M-d'T'H:m:s.SSSX" ))
      .drop("timestamp")
      .select("received_date", "event_timestamp", "device", "CO2_level", "humidity", "temperature")
    //TODO column names should be saved somewhere (as constants in their respective data models for example)
  }


}

