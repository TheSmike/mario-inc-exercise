package it.scarpenti.marioinc
package utils

import java.text.SimpleDateFormat
import java.util.Calendar

object DateUtils {

  val dashedFormatter = new SimpleDateFormat("yyy-MM-dd")

  def dashedFormat(cal: Calendar) : String = {
    dashedFormatter.format(cal.getTime)
  }

}
