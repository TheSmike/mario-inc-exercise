package it.scarpenti.marioinc
package model

import java.time.{Instant, LocalDate}


case class Device(
                   received_date: LocalDate,
                   event_timestamp: Instant,
                   device: String,
                   CO2_level: Long,
                   humidity: Long,
                   temperature: Long,
                   event_date: LocalDate
                 )

object Device {
  final val RECEIVED_DATE = "received_date"
  final val EVENT_TIMESTAMP = "event_timestamp"
  final val DEVICE = "device"
  final val CO2_LEVEL = "CO2_level"
  final val HUMIDITY = "humidity"
  final val TEMPERATURE = "temperature"
  final val EVENT_DATE = "event_date"
}

