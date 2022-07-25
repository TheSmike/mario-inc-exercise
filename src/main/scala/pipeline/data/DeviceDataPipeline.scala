package it.scarpenti.marioinc
package pipeline.data

import model.{Device, RawDevice}
import utils.DateUtils.toLocalDate

import io.delta.tables.DeltaTable
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col

import java.time.LocalDate
import java.time.temporal.ChronoUnit

object DeviceDataPipeline extends SparkApp[DeviceDataContext] {

  override def init(): DeviceDataContext = new DeviceDataContext()

  override def run(context: DeviceDataContext): Unit = {
    val receivedDate = toLocalDate(context.receivedDate)

    val raw = readDfRawDataTable()
    val rawFiltered = filterRawData(receivedDate, raw)
    val rawProjected = projectRawData(rawFiltered)

    val oldData = readDeltaDataTable()
    mergeRawIntoDeviceData(receivedDate, rawProjected, oldData)
  }

  private def mergeRawIntoDeviceData(receivedDate: LocalDate, rawProjected: DataFrame, oldData: DeltaTable) = {
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
      .execute()

    //TODO handle the force mode? how?
  }

  private def readDfRawDataTable() = {
    session.read.format("delta").table(config.rawDataTableName)
  }

  private def filterRawData(receivedDate: LocalDate, bronze: DataFrame): Dataset[Row] = {
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

  private def readDeltaDataTable() = DeltaTable.forName(config.dataTableName)

}

