package it.scarpenti.marioinc
package pipeline.info

import config.AppConfig

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class DeviceInfoLogic(session: SparkSession, config: AppConfig) {

  def run(): Unit = {
    val csv = readInfoDataFromLandingZone(session)
    writeInfoData(csv)
  }

  private def writeInfoData(csv: DataFrame): Unit = {
    csv.write.format("delta").mode(SaveMode.Overwrite).saveAsTable(config.infoTableName)
  }

  private def readInfoDataFromLandingZone(session: SparkSession) = {
    session.read.format("csv").option("header", "true").load(config.infoLandingZonePath)
  }

}
