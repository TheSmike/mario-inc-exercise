package it.scarpenti.marioinc
package model

case class RawData(
                    CO2_level: Long,
                    device: String,
                    humidity: Long,
                    temperature: Long,
                    timestamp: String, //ZonedDateTime?
                    received: String // Date?
                  )