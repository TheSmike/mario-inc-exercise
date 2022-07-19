package it.scarpenti.marioinc
package pipeline.init

import com.typesafe.config._


class CreateTablesContext(config: Config, args: Array[String]) {

  def this(args: Array[String]) {
    this(ConfigFactory.load(), args)
  }

  final private val infoPrefix = "device-info"
  final private val dataPrefix = "device-data"
  final private val rawDataPrefix = "raw-device-data"
  final private val reportPrefix = "device-report"

  config.checkValid(ConfigFactory.defaultReference(), infoPrefix, dataPrefix, reportPrefix)

  val database = config.getString("database")
  val infoTableName = config.getString(s"$infoPrefix.full-table-name")
  val dataTableName = config.getString(s"$dataPrefix.full-table-name")
  val reportTableName = config.getString(s"$reportPrefix.full-table-name")
  val rawDataTableName = config.getString(s"$rawDataPrefix.full-table-name")

  val dataPath = config.getString(s"$dataPrefix.output-path")
}

