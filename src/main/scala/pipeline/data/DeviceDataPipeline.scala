package it.scarpenti.marioinc
package pipeline.data

import utils.DateUtils.toLocalDate

import io.delta.tables.DeltaTable

object DeviceDataPipeline extends SparkApp[DeviceDataContext] {

  override def init(): DeviceDataContext = new DeviceDataContext()

  override def run(context: DeviceDataContext): Unit = {
    val receivedDate = toLocalDate(context.receivedDate)
    val rawInputDs = readDfRawDataTable()
    val outputDeltaTable = readDeltaDataTable()
    new DeviceDataLogic(session, config.maxDelay).run(receivedDate, rawInputDs, outputDeltaTable)
  }

  private def readDfRawDataTable() = {
    session.read.format("delta").table(config.rawDataTableName)
  }

  private def readDeltaDataTable() = DeltaTable.forName(config.dataTableName)


}

