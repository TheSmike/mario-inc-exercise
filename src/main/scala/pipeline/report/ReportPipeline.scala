package it.scarpenti.marioinc
package pipeline.report

import utils.spark.SparkApp

import org.apache.spark.sql._
import org.apache.spark.sql.functions.{avg, col, date_format}

object ReportPipeline extends SparkApp[ReportContext] {

  override def init(): ReportContext = new ReportContext()


  override def run(context: ReportContext): Unit = {
    val cleansedData = session.read.format("delta").table(context.dataTableName)
    val info = session.read.format("delta").table(context.infoTableName)
      .withColumnRenamed("code", "device")

    val filtered = cleansedData
      .filter(date_format(col("event_date"), "yyyyMM").between(
        context.yearMonthFrom,
        context.yearMonthTo)
      )  //TODO verify if this filter is pushed down to partition, if not a better partition strategy would be (year, month, day)

    val joined = filtered.join(info, "device")
    val grouped = groupByMonthAndArea(joined)

    grouped
      .write
      .format("delta")
      .mode(SaveMode.Overwrite)
      .option("replaceWhere", s"year_month between '${context.yearMonthFrom}' and  '${context.yearMonthTo}'")
      .saveAsTable(context.reportTableName)

  }

  def groupByMonthAndArea(joined: DataFrame): DataFrame = {
    joined
      .groupBy(
        date_format(col("event_date"), "yyyyMM").alias("year_month"),
        col("area")
      )
      .agg(
        avg(col("CO2_level")).alias("CO2_level_avg"),
        avg(col("humidity")).alias("humidity_avg"),
        avg(col("temperature")).alias("temperature_avg")
      )
  }

}