package it.scarpenti.marioinc
package pipeline.data

import config.AppConfig
import model.{Device, RawDevice}
import utils.Profiles.{PRODUCTION, STAGING}

import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, datediff}

import java.time.LocalDate
import java.time.temporal.ChronoUnit

class DeviceDataLogic(session: SparkSession, config: AppConfig, profile: String) {

  def run(receivedDate: LocalDate): Unit = {
    val eventDateFrom = receivedDate.plus(-config.maxDelay, ChronoUnit.DAYS)
    val eventDateTo = receivedDate

    val rawInputDs = readRawDataTableSlice(eventDateFrom, eventDateTo)
    val rawFiltered = filterRawData(rawInputDs)
    val rawProjected = projectRawData(rawFiltered)

    writeCleansedData(rawProjected, eventDateFrom, eventDateTo)
    optimizeTableToReadingByDevice()
  }

  private def readRawDataTableSlice(eventDateFrom: LocalDate, eventDateTo: LocalDate) = {
    session.read.format("delta").table(config.rawDataTableName)
      .filter(col(RawDevice.EVENT_DATE).between(eventDateFrom, eventDateTo))
  }

  def filterRawData(bronze: DataFrame): Dataset[Row] = {
    bronze
      .filter(datediff(col(RawDevice.RECEIVED), col(RawDevice.EVENT_DATE)) <= 1)
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
      .option("replaceWhere", s"${Device.EVENT_DATE} >= '$eventDateFrom' and ${Device.EVENT_DATE} <= '$eventDateTo'")
      .saveAsTable(config.dataTableName)
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
