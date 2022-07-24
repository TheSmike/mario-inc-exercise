package it.scarpenti.marioinc
package model

import utils.DateUtils.{fromDashedFormat, toTimestamp}

import java.sql.{Date, Timestamp}

case class RawDevice(
                      received: String,
                      device: String,
                      timestamp: Timestamp,
                      CO2_level: Long,
                      humidity: Long,
                      temperature: Long,
                      event_date: Date,
                    )

object RawDevice {

  final val RECEIVED = "received"
  final val DEVICE = "device"
  final val TIMESTAMP = "timestamp"
  final val CO2_LEVEL = "CO2_level"
  final val HUMIDITY = "humidity"
  final val TEMPERATURE = "temperature"
  final val EVENT_DATE = "event_date"

  def apply(received: String,
            device: String,
            timestamp: String,
            CO2_level: Long,
            humidity: Long,
            temperature: Long,
            event_date: String) =
    new RawDevice(received, device, toTimestamp(timestamp), CO2_level, humidity, temperature, fromDashedFormat(event_date))

}