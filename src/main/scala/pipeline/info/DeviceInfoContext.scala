package it.scarpenti.marioinc
package pipeline.info

import com.typesafe.config._


class DeviceInfoContext(config: Config, args: Array[String]) {

  def this(args: Array[String]) {
    this(ConfigFactory.load(), args)
  }

  final private val prefix = "device-info"

  config.checkValid(ConfigFactory.defaultReference(), prefix)

  val appName = config.getString(s"$prefix.name")
  val processingDate = config.getInt(s"$prefix.processing-date")
  val inputPath = config.getString(s"$prefix.input-path")
  val outputPath = config.getString(s"$prefix.output-path")

}

