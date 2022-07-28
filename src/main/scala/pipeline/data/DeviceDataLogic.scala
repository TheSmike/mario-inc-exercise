package it.scarpenti.marioinc
package pipeline.data

import config.AppConfig
import model.{Device, RawDevice}
import utils.Profiles.{PRODUCTION, STAGING}

import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import java.time.LocalDate
import java.time.temporal.ChronoUnit

class DeviceDataLogic(session: SparkSession, config: AppConfig, profile: String) {

  def run(receivedDate: LocalDate): Unit = {
    val rawInputDs = readDfRawDataTable()
    val outputDeltaTable = readDeltaDataTable()
    val rawFiltered = filterRawData(receivedDate, rawInputDs)
    val rawProjected = projectRawData(rawFiltered)

    mergeRawIntoDeviceData(receivedDate, rawProjected, outputDeltaTable)
    optimizeTableToReadingByDevice()
  }

  private def readDfRawDataTable() = {
    session.read.format("delta").table(config.rawDataTableName)
  }

  private def readDeltaDataTable() = DeltaTable.forName(config.dataTableName)

  private def mergeRawIntoDeviceData(receivedDate: LocalDate, rawProjected: DataFrame, oldData: DeltaTable): Unit = {
    oldData
      .as("data")
      .merge(
        rawProjected.as("raw"),
        col("data." + Device.EVENT_DATE).between(receivedDate.plus(-config.maxDelay, ChronoUnit.DAYS), receivedDate)
          && (col("data." + Device.DEVICE) === col("raw." + Device.DEVICE))
          && (col("data." + Device.EVENT_TIMESTAMP) === col("raw." + Device.EVENT_TIMESTAMP))
      )
      .whenNotMatched()
      .insertAll()
      .execute() //TODO read 2 partitions
    //TODO handle the force mode? how? when matched than update
  }

  def filterRawData(receivedDate: LocalDate, bronze: DataFrame): Dataset[Row] = {
    bronze
      .filter(col(RawDevice.EVENT_DATE).between(
        receivedDate.plus(-config.maxDelay, ChronoUnit.DAYS),
        receivedDate
      ))
      .filter(col(RawDevice.RECEIVED) === receivedDate)
      .dropDuplicates(RawDevice.DEVICE, RawDevice.TIMESTAMP)
  }

  def projectRawData(filtered: Dataset[Row]): DataFrame = {
    filtered
      .withColumnRenamed(RawDevice.RECEIVED, Device.RECEIVED_DATE)
      .withColumnRenamed(RawDevice.TIMESTAMP, Device.EVENT_TIMESTAMP)
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
