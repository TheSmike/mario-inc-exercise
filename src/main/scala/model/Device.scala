package it.scarpenti.marioinc
package model

import java.sql.Timestamp
import java.util.Date

case class Device(
                   received_data: Date,
                   event_timestamp: Timestamp,
                   device: String,
                   CO2_level: Int,
                   humidity: Int,
                   temperature: Int,
                   event_date: Date
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

