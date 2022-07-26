package it.scarpenti.marioinc
package pipeline.data

import model.{Device, RawDevice}

import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import java.time.LocalDate
import java.time.temporal.ChronoUnit

class DeviceDataLogic(session: SparkSession, maxDelay: Int) {

  def run(receivedDate: LocalDate, rawInputDs: DataFrame, outputDeltaTable: DeltaTable): Unit = {
    val rawFiltered = filterRawData(receivedDate, rawInputDs)
    val rawProjected = projectRawData(rawFiltered)

    mergeRawIntoDeviceData(receivedDate, rawProjected, outputDeltaTable)
  }

  private def mergeRawIntoDeviceData(receivedDate: LocalDate, rawProjected: DataFrame, oldData: DeltaTable): Unit = {
    oldData
      .as("data")
      .merge(
        rawProjected.as("raw"),
        col("data." + Device.EVENT_DATE).between(receivedDate.plus(-maxDelay, ChronoUnit.DAYS), receivedDate)
          && (col("data." + Device.DEVICE) === col("raw." + Device.DEVICE))
          && (col("data." + Device.EVENT_TIMESTAMP) === col("raw." + Device.EVENT_TIMESTAMP))
      )
      .whenNotMatched()
      .insertAll()
      .execute()
    //TODO handle the force mode? how? when matched than update
  }

  def filterRawData(receivedDate: LocalDate, bronze: DataFrame): Dataset[Row] = {
    bronze
      .filter(col(RawDevice.EVENT_DATE).between(
        receivedDate.plus(-maxDelay, ChronoUnit.DAYS),
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

}
