package it.scarpenti.marioinc
package utils

import java.time.{Instant, LocalDate}

object DateUtils {

  def toLocalDate(dateStr: String): LocalDate = {
    LocalDate.parse(dateStr)
  }

  def toInstant(timestampStr: String): Instant = {
    Instant.parse(timestampStr)
  }

}
