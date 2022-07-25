package it.scarpenti.marioinc
package pipeline.rawdata

import model.RawDevice

import org.apache.spark.sql.functions.{col, to_timestamp}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode}

object RawDeviceDataPipeline extends SparkApp[RawDeviceDataContext] {

  override def init(): RawDeviceDataContext = new RawDeviceDataContext()

  override def run(context: RawDeviceDataContext): Unit = {
    val json = readRawDataFromLandingZone(context.receivedDate)
    val transformed = timestampColToTimestampType(json)
    writeRawData(context.receivedDate, transformed)
  }

  private def writeRawData(receivedDateStr: String, transformed: DataFrame): Unit = {
    transformed
      .write
      .format("delta")
      .mode(SaveMode.Overwrite)
      .option("replaceWhere", s"received = '$receivedDateStr'")
      .saveAsTable(config.rawDataTableName)
    //TODO implement force mode. currently is always in "force mode"
  }

  private def timestampColToTimestampType(json: Dataset[Row]) = {
    json.withColumn(RawDevice.TIMESTAMP, to_timestamp(col(RawDevice.TIMESTAMP)))
  }

  private def readRawDataFromLandingZone(receivedDateStr: String) = {
    session.read.json(config.rawDataLandingZonePath)
      .filter(col("received") === receivedDateStr)
  }
}
