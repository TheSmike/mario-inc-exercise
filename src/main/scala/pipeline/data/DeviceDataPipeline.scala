package it.scarpenti.marioinc
package pipeline.data

import model.Device
import utils.DateUtils.toLocalDate

import io.delta.tables.DeltaTable

object DeviceDataPipeline extends SparkApp[DeviceDataContext] {

  override def init(): DeviceDataContext = new DeviceDataContext()

  override def run(context: DeviceDataContext): Unit = {
    val receivedDate = toLocalDate(context.receivedDate)
    val rawInputDs = readDfRawDataTable()
    val outputDeltaTable = readDeltaDataTable()
    new DeviceDataLogic(session, config.maxDelay).run(receivedDate, rawInputDs, outputDeltaTable)
    optimizeTableToReadingByDevice(context.profile)
  }

  private def readDfRawDataTable() = {
    session.read.format("delta").table(config.rawDataTableName)
  }

  private def readDeltaDataTable() = DeltaTable.forName(config.dataTableName)

  /**
   * This specific delta table optimization allow to improve reading performance when filtering by device.
   * It's disabled in the local environment because it works only on databricks. you shouldn't need it in local.
   * @param profile
   */
  def optimizeTableToReadingByDevice(profile: String): Unit = {
    if (profile != "local")
      session.sql(s"OPTIMIZE ${config.dataTableName} ZORDER BY (${Device.DEVICE})")
  }

}

