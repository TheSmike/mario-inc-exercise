package it.scarpenti.marioinc
package utils

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat
import java.util.{Calendar, TimeZone}

object DateUtils {

  val dashedFormatter = new SimpleDateFormat("yyy-MM-dd")
  val tsUtcFormatter = new SimpleDateFormat("yyy-MM-dd'T'HH:mm:ss.SSSX")
  tsUtcFormatter.setTimeZone(TimeZone.getTimeZone("GMT"))

  def dashedFormat(cal: Calendar): String = {
    dashedFormatter.format(cal.getTime)
  }

  def fromDashedFormat(dateStr: String): Date = {
    Date.valueOf(dateStr)
  }

  def toTimestamp(timestampStr: String): Timestamp = {
    Timestamp.from(tsUtcFormatter.parse(timestampStr).toInstant)
  }

  def fromTimestamp(date: Timestamp): String = {
    tsUtcFormatter.format(date)
  }

}
