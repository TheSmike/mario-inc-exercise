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

  val eventDate = args(0) //TODO introduce something better to parse args (and to validate them)
  //TODO: A possible improvement consist into handle a range of dates also, to easily (re)compute a lot of months


}

