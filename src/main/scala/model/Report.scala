package it.scarpenti.marioinc
package model

case class Report(
                   CO2_level: Long,
                   device: String,
                   humidity: Long,
                   temperature: Long,
                   timestamp: String,
                   received: String
                 )

object Report {
  final val YEAR_MONTH = "year_month"
  final val AREA = "area"
  final val CO2_LEVEL_AVG = "CO2_level_avg"
  final val HUMIDITY_AVG = "humidity_avg"
  final val TEMPERATURE_AVG = "temperature_avg"
}