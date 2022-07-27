package it.scarpenti.marioinc
package config

import com.typesafe.config.{Config, ConfigFactory}

class ConfigReader(config: Config) {

  def this(profile: String) {
    this(ConfigFactory.load(s"application-$profile"))
  }

  final private val infoPrefix = "device-info"
  final private val dataPrefix = "device-data"
  final private val rawDataPrefix = "raw-device-data"
  final private val reportPrefix = "device-report"

  config.checkValid(ConfigFactory.defaultReference())

  def read(): AppConfig = {
    AppConfig(
      config.getString("database"),
      config.getString("database-path"),
      config.getString(s"$infoPrefix.landing-zone-path"),
      config.getString(s"$infoPrefix.table"),
      config.getString(s"$infoPrefix.output-path"),
      config.getString(s"$rawDataPrefix.landing-zone-path"),
      config.getString(s"$rawDataPrefix.table"),
      config.getString(s"$rawDataPrefix.output-path"),
      config.getString(s"$dataPrefix.table"),
      config.getString(s"$dataPrefix.output-path"),
      config.getInt(s"$dataPrefix.max-delay"),
      config.getString(s"$reportPrefix.table"),
      config.getString(s"$reportPrefix.output-path"),
    )
  }

}
