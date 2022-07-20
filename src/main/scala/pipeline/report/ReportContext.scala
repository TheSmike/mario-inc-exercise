package it.scarpenti.marioinc
package pipeline.report

import com.typesafe.config._


class ReportContext(config: Config, args: Array[String]) {

  def this(args: Array[String]) {
    this(ConfigFactory.load(), args)
  }

  final private val prefix = "device-report"
  config.checkValid(ConfigFactory.defaultReference(), prefix)

  val appName = config.getString(s"$prefix.name")
  val dataTableName = config.getString("device-data.full-table-name")
  val reportTableName = config.getString(s"$prefix.full-table-name")
  val infoTableName = config.getString("device-info.full-table-name")

  val yearMonthFrom = args(0)
  val yearMonthTo = args(1)
  //TODO introduce something better to parse args (and to validate them)

}

