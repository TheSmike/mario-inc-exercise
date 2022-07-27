package it.scarpenti.marioinc
package pipeline.report

object ReportPipeline extends SparkApp[ReportContext] {

  override def init(): ReportContext = new ReportContext()

  override def run(context: ReportContext): Unit = {
    new ReportLogic(session, config).run(context.intYearMonthFrom, context.intYearMonthTo)

  }

}