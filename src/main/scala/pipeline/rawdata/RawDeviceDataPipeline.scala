package it.scarpenti.marioinc
package pipeline.rawdata

import model.RawDevice
import utils.DateUtils.dashedFormat

import org.apache.spark.sql.functions.{col, to_timestamp}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode}

object RawDeviceDataPipeline extends SparkApp[RawDeviceDataContext] {

  override def init(): RawDeviceDataContext = new RawDeviceDataContext()

  override def run(context: RawDeviceDataContext): Unit = {
    val receivedDateStr = dashedFormat(context.receivedDate)

    val json = readRawDataFromLandingZone(receivedDateStr)
    val transformed = timestampColToTimestampType(json)
    writeRawData(receivedDateStr, transformed)
  }

  private def writeRawData(receivedDateStr: String, transformed: DataFrame): Unit = {
    transformed
      .write
      .format("delta")
      .mode(SaveMode.Overwrite)
      .option("replaceWhere", s"received = '$receivedDateStr'")
      .saveAsTable(config.rawDataTableName)
  }

  private def timestampColToTimestampType(json: Dataset[Row]) = {
    json.withColumn(RawDevice.TIMESTAMP, to_timestamp(col(RawDevice.TIMESTAMP)))
  }

  private def readRawDataFromLandingZone(receivedDateStr: String) = {
    session.read.json(config.rawDataLandingZonePath)
      .filter(col("received") === receivedDateStr)
  }
}
