package it.scarpenti.marioinc
package pipeline.rawdata

import config.AppConfig
import model.RawDevice

import org.apache.spark.sql.functions.{col, to_timestamp}
import org.apache.spark.sql._

class RawDataLogic(session: SparkSession, config: AppConfig, force: Boolean) {

  def run(receivedDate: String): Unit = {
    val json = readRawDataFromLandingZone(receivedDate)
    val transformed = timestampColToTimestampType(json)
    writeRawData(receivedDate, transformed)
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
