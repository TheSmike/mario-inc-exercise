package it.scarpenti.marioinc
package pipeline.data

import config.AppConfig
import model.{Device, RawDevice}
import utils.Profiles.{PRODUCTION, STAGING}

import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions.col
import org.apache.spark.sql._

import java.time.LocalDate
import java.time.temporal.ChronoUnit

class DeviceDataLogic(session: SparkSession, config: AppConfig, profile: String) {

  def run(receivedDate: LocalDate): Unit = {
    val eventDateFrom = receivedDate.plus(-config.maxDelay, ChronoUnit.DAYS)
    val eventDateTo = receivedDate

    val rawInputDs = readDfRawDataTable()
    val rawFiltered = filterRawData(rawInputDs, eventDateFrom, eventDateTo)
    val rawProjected = projectRawData(rawFiltered)

    writeCleansedData(rawProjected, eventDateFrom, eventDateTo)
    optimizeTableToReadingByDevice()
  }

  private def readDfRawDataTable() = {
    session.read.format("delta").table(config.rawDataTableName)
  }

  def filterRawData(bronze: DataFrame, eventDateFrom: LocalDate, eventDateTo: LocalDate): Dataset[Row] = {
    bronze
      .filter(col(RawDevice.EVENT_DATE).between(eventDateFrom, eventDateTo))
      .dropDuplicates(RawDevice.DEVICE, RawDevice.TIMESTAMP)
  }

  def projectRawData(filtered: Dataset[Row]): DataFrame = {
    filtered
      .withColumnRenamed(RawDevice.RECEIVED, Device.RECEIVED_DATE)
      .withColumnRenamed(RawDevice.TIMESTAMP, Device.EVENT_TIMESTAMP)
  }

  def writeCleansedData(rawProjected: DataFrame, eventDateFrom: LocalDate, eventDateTo: LocalDate): Unit = {
    rawProjected
      .write
      .format("delta")
      .mode(SaveMode.Overwrite)
      .option("replaceWhere", s"${Device.EVENT_DATE} between '$eventDateFrom' and '$eventDateTo")
      .saveAsTable(config.rawDataTableName)
  }

  /**
   * This specific delta table optimization allow to improve reading performance when filtering by device.
   * It's useless in the local environment because it works only on databricks. Anyway you shouldn't need it in local.
   */
  def optimizeTableToReadingByDevice(): Unit = {
    if (profile == STAGING || profile == PRODUCTION)
      session.sql(s"OPTIMIZE ${config.dataTableName} ZORDER BY (${Device.DEVICE})")
  }

}
