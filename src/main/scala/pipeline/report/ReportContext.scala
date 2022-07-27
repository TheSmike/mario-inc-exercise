package it.scarpenti.marioinc
package pipeline.report

import org.backuity.clist.arg

class ReportContext() extends AbstractContext("report") {
  var yearMonthFrom: String = arg[String](description = "The starting month from which to calculate the report, in the form yyy-MM. i.e.: 2020-01")
  var yearMonthTo: String = arg[String](description = "The last month to use calculate the report. It will be in the form yyy-MM. i.e.: 2020-11")

  def intYearMonthFrom: Int = toInt(yearMonthFrom)

  def intYearMonthTo: Int = toInt(yearMonthTo)

  //TODO a date parser is certainly a better solutions for the method below!
  private def toInt(yearMonth: String): Int = yearMonth.replace("-", "").toInt


}

