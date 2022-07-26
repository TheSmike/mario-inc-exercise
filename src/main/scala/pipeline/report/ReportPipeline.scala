package it.scarpenti.marioinc
package pipeline.report

import model.{Device, Report}

import org.apache.spark.sql._
import org.apache.spark.sql.functions.{avg, col, date_format}
import org.apache.spark.sql.types.IntegerType

object ReportPipeline extends SparkApp[ReportContext] {

  override def init(): ReportContext = new ReportContext()

  override def run(context: ReportContext): Unit = {
    validateInput(context)

    val data = readData
    val info = readInfo

    val infoProj = projectInfo(info)
    val filtered = filterData(context, data)
    val joined = joinDataAndInfo(infoProj, filtered)
    val grouped = groupByMonthAndArea(joined)

    writeReport(context, grouped)
  }

  private def writeReport(context: ReportContext, grouped: DataFrame): Unit = {
    grouped
      .write
      .format("delta")
      .mode(SaveMode.Overwrite)
      .option("replaceWhere", s"${Report.YEAR_MONTH} between ${context.intYearMonthFrom} and ${context.intYearMonthTo}")
      .saveAsTable(config.reportTableName)
  }

  private def joinDataAndInfo(infoProj: DataFrame, filtered: DataFrame) = {
    filtered.join(infoProj, "device")
  }

  private def validateInput(context: ReportContext): Unit = {
    //TODO validate input parameters (are they really months?)
  }

  private def filterData(context: ReportContext, cleansedData: DataFrame) = {
    cleansedData
      .filter(date_format(col(Device.EVENT_DATE), "yyyyMM").cast(IntegerType).between(
        context.intYearMonthFrom,
        context.intYearMonthTo)
      )
    //TODO Verify if this filter is pushed down to partition, if not a better partition strategy would be (year, month, day)
  }

  private def projectInfo(info: DataFrame) = {
    info.withColumnRenamed("code", "device")
  }

  private def readInfo = {
    session.read.format("delta").table(config.infoTableName)
  }

  private def readData = {
    session.read.format("delta").table(config.dataTableName)
  }

  def groupByMonthAndArea(joined: DataFrame): DataFrame = {
    joined
      .groupBy(
        date_format(col(Device.EVENT_DATE), "yyyyMM").cast(IntegerType).alias(Report.YEAR_MONTH),
        col(Report.AREA)
      )
      .agg(
        avg(col(Device.CO2_LEVEL)).alias(Report.CO2_LEVEL_AVG),
        avg(col(Device.HUMIDITY)).alias(Report.HUMIDITY_AVG),
        avg(col(Device.TEMPERATURE)).alias(Report.TEMPERATURE_AVG)
      )
  }

}