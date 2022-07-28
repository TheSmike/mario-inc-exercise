package it.scarpenti.marioinc
package pipeline.init

object CreateTablesPipeline extends SparkApp[CreateTablesContext] {

  override def init(): CreateTablesContext = new CreateTablesContext()

  override def run(context: CreateTablesContext): Unit = {
    new CreateTablesLogic(session, config, context.force).run()
  }

}
