package it.scarpenti.marioinc
package model

import utils.DateUtils.{toInstant, toLocalDate}

import java.time.{Instant, LocalDate}

case class RawDevice(
                      received: LocalDate,
                      device: String,
                      timestamp: Instant,
                      CO2_level: Long,
                      humidity: Long,
                      temperature: Long,
                      event_date: LocalDate,
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
    new RawDevice(toLocalDate(received), device, toInstant(timestamp), CO2_level, humidity, temperature, toLocalDate(event_date))

}