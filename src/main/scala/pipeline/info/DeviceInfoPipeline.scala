package it.scarpenti.marioinc
package pipeline.info

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object DeviceInfoPipeline extends SparkApp[DeviceInfoContext] {

  override def init(): DeviceInfoContext = new DeviceInfoContext()

  override def run(context: DeviceInfoContext): Unit = {
    val csv = readInfoDataFromLandingZone(session)
    logger.info("schema is: " + csv.schema)
    writeInfoData(csv)
  }

  private def writeInfoData(csv: DataFrame): Unit = {
    csv.write.format("delta").mode(SaveMode.Overwrite).saveAsTable(config.infoTableName)
  }

  private def readInfoDataFromLandingZone(session: SparkSession) = {
    session.read.format("csv").option("header", "true").load(config.infoLandingZonePath)
  }
}
