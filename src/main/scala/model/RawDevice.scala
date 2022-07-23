package it.scarpenti.marioinc
package model

case class RawDevice(
                      CO2_level: Long,
                      device: String,
                      humidity: Long,
                      temperature: Long,
                      timestamp: String, //ZonedDateTime?
                      received: String // Date?
                    )

object RawDevice {
  final val RECEIVED = "received"
  final val DEVICE = "device"
  final val TIMESTAMP = "timestamp"
  final val CO2_LEVEL = "CO2_level"
  final val HUMIDITY = "humidity"
  final val TEMPERATURE = "temperature"
  final val EVENT_DATE = "event_date"
}