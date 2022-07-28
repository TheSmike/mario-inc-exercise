package it.scarpenti.marioinc
package pipeline.rawdata

import config.AppConfig
import model.RawDevice

import org.apache.spark.sql.functions.{col, to_timestamp}
import org.apache.spark.sql._

class RawDataLogic(session: SparkSession, config: AppConfig) {

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
      .option("replaceWhere", s"${RawDevice.RECEIVED} = '$receivedDateStr'")
      .saveAsTable(config.rawDataTableName)
    //TODO fully implement force mode. currently is always in "force mode"
  }

  private def timestampColToTimestampType(json: Dataset[Row]) = {
    json.withColumn(RawDevice.TIMESTAMP, to_timestamp(col(RawDevice.TIMESTAMP)))
    //TODO: an alternative could be forcing a schema
  }

  private def readRawDataFromLandingZone(receivedDateStr: String) = {
    session.read.json(config.rawDataLandingZonePath)
      .filter(col("received") === receivedDateStr)
  }

}
