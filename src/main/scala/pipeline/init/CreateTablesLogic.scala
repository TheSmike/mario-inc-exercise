package it.scarpenti.marioinc
package pipeline.init

import config.AppConfig
import model.{Device, Info, RawDevice, Report}

import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

class CreateTablesLogic(session: SparkSession, config: AppConfig, force: Boolean) {

  def run(): Unit = {
    createDatabase(session)
    createInfoTable(force)
    createRawDataTable(force)
    createDataTable(force)
    createReportTable(force)
  }

  private def createDatabase(session: SparkSession) = {
    session.sql(s"CREATE DATABASE IF NOT EXISTS mario LOCATION '${config.databasePath}' ")
  }

  private def createInfoTable(force: Boolean) = {
    createTableBuilder(force)
      .tableName(config.infoTableName)
      .addColumn(Info.CODE, StringType)
      .addColumn(Info.TYPE, StringType)
      .addColumn(Info.AREA, StringType)
      .addColumn(Info.CUSTOMER, StringType)
      .location(config.infoOutputPath)
      .execute()
  }

  private def createRawDataTable(force: Boolean) = {
    createTableBuilder(force)
      .tableName(config.rawDataTableName)
      .addColumn(RawDevice.RECEIVED, DateType)
      .addColumn(RawDevice.DEVICE, StringType)
      .addColumn(RawDevice.TIMESTAMP, TimestampType)
      .addColumn(RawDevice.CO2_LEVEL, LongType)
      .addColumn(RawDevice.HUMIDITY, LongType)
      .addColumn(RawDevice.TEMPERATURE, LongType)
      .addColumn(
        DeltaTable.columnBuilder(RawDevice.EVENT_DATE)
          .dataType(DateType)
          .generatedAlwaysAs(s"CAST(${RawDevice.TIMESTAMP} AS DATE)")
          .build())
      .partitionedBy(RawDevice.EVENT_DATE)
      .location(config.rawOutputPath)
      .execute()
  }

  private def createDataTable(force: Boolean): Unit = {
    createTableBuilder(force)
      .tableName(config.dataTableName)
      .addColumn(Device.RECEIVED_DATE, DateType)
      .addColumn(Device.EVENT_TIMESTAMP, TimestampType)
      .addColumn(Device.DEVICE, StringType)
      .addColumn(Device.CO2_LEVEL, LongType)
      .addColumn(Device.HUMIDITY, LongType)
      .addColumn(Device.TEMPERATURE, LongType)
      .addColumn(
        DeltaTable.columnBuilder(Device.EVENT_DATE)
          .dataType(DateType)
          .generatedAlwaysAs(s"CAST(${Device.EVENT_TIMESTAMP} AS DATE)")
          .build())
      .partitionedBy(Device.EVENT_DATE, Device.DEVICE)
      .location(config.dataOutputPath)
      .execute()
  }

  private def createTableBuilder(force: Boolean) = {
    if (force) DeltaTable.createOrReplace(session)
    else DeltaTable.createIfNotExists(session)
  }

  private def createReportTable(force: Boolean): Unit = {
    createTableBuilder(force)
      .tableName(config.reportTableName)
      .addColumn(Report.YEAR_MONTH, IntegerType)
      .addColumn(Report.AREA, StringType)
      .addColumn(Report.CO2_LEVEL_AVG, DoubleType)
      .addColumn(Report.HUMIDITY_AVG, DoubleType)
      .addColumn(Report.TEMPERATURE_AVG, DoubleType)
      .location(config.reportOutputPath)
      .execute()
  }

}
