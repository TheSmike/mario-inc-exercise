package it.scarpenti.marioinc
package model

import java.sql.Timestamp
import java.time.ZonedDateTime
import java.util.Date

case class DeviceData(
                       received_data: Date,
                       event_timestamp: Timestamp,
                       device: String,
                       CO2_level: Int,
                       humidity: Int,
                       temperature: Int,
                       event_date: Date
                  ){
}

