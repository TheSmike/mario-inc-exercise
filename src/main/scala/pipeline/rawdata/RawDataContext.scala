package it.scarpenti.marioinc
package pipeline.rawdata

import com.typesafe.config._


class RawDataContext(config: Config, args: Array[String]) {

  def this(args: Array[String]) {
    this(ConfigFactory.load(), args)
  }

  final private val prefix = "device-data"
  config.checkValid(ConfigFactory.defaultReference(), prefix)

  val appName = config.getString(s"$prefix.name")
  val inputPath = config.getString(s"$prefix.input-path")
  val outputPath = config.getString(s"$prefix.output-path")
  val fullTableName = config.getString(s"$prefix.full-table-name")
  val partitions =  config.getInt(s"$prefix.partitions")

  val receivedDate = args(0) //TODO introduce something better to parse args (and to validate them)

}

