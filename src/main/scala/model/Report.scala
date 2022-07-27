package it.scarpenti.marioinc
package model

case class Report(
                   year_month: Int,
                   area: String,
                   CO2_level_avg: Long,
                   humidity_avg: Long,
                   temperature_avg: Long,
                 )

object Report {
  final val YEAR_MONTH = "year_month"
  final val AREA = "area"
  final val CO2_LEVEL_AVG = "CO2_level_avg"
  final val HUMIDITY_AVG = "humidity_avg"
  final val TEMPERATURE_AVG = "temperature_avg"
}