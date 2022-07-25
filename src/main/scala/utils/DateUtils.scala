package it.scarpenti.marioinc
package utils

import java.text.SimpleDateFormat
import java.time.{Instant, LocalDate}
import java.util.TimeZone

object DateUtils {

  val dashedFormatter = new SimpleDateFormat("yyy-MM-dd")
  val tsUtcFormatter = new SimpleDateFormat("yyy-MM-dd'T'HH:mm:ss.SSSX")
  tsUtcFormatter.setTimeZone(TimeZone.getTimeZone("GMT"))

  def toLocalDate(dateStr: String): LocalDate = {
    LocalDate.parse(dateStr)
  }

  def toInstant(timestampStr: String): Instant = {
    Instant.parse(timestampStr)
  }

}
