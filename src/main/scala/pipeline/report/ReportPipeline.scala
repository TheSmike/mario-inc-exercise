package it.scarpenti.marioinc
package pipeline.report

import utils.spark.SparkApp

import org.apache.spark.sql._
import org.apache.spark.sql.functions.{avg, col, date_format}

object ReportPipeline extends SparkApp[ReportContext] {

  override def init(args: Array[String]): ReportContext = new ReportContext(args)


  override def run(session: SparkSession, context: ReportContext): Unit = {
    val cleansedData = session.read.format("delta").table(context.dataTableName)
    val info = session.read.format("delta").table(context.infoTableName)
      .withColumnRenamed("code", "device")

    val yearMonth = context.eventDate.substring(0, 4) + context.eventDate.substring(5, 7)
    logger.info(s"yearMonth is $yearMonth")

    val joined = cleansedData.join(info, "device")
    val grouped = groupByMonthAndArea(joined)

    grouped
      .write
      .format("delta")
      .mode(SaveMode.Overwrite)
      .option("replaceWhere", s"year_month = '$yearMonth'")
      .saveAsTable(context.reportTableName)

  }

  def groupByMonthAndArea(joined: DataFrame) = {
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