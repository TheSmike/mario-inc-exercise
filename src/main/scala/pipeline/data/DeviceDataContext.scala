package it.scarpenti.marioinc
package pipeline.data

import utils.spark.AbstractContext

import com.typesafe.config._


class DeviceDataContext(config: Config) extends AbstractContext("device-data") {

  def this() {
    this(ConfigFactory.load())
  }

  final private val prefix = "device-data"
  config.checkValid(ConfigFactory.defaultReference(), prefix)

  val appName = config.getString(s"$prefix.name")
  val inputPath = config.getString(s"$prefix.input-path")
  val outputPath = config.getString(s"$prefix.output-path")
  val dataTableName = config.getString(s"$prefix.full-table-name")
  val rawDataTableName = config.getString("raw-device-data.full-table-name")

  val receivedDate = ""
//  val receivedDate = args(0) //TODO introduce something better to parse args (and to validate them)

}

