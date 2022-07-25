package it.scarpenti.marioinc
package pipeline.init

import model.{Device, Info, RawDevice, Report}

import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

/**
 * This pipeline is used to create data tables in the data catalog the first time.
 * This operation could also be done by launching SQL scripts directly into SQL Engine system.
 * I usually prefer write files containing SQL statements to create tables and run them on
 * an SQL Engine the first time in every environment, if this is the official way to create tables,
 * also the sql scripts should be parametrized.
 */
object CreateTablesPipeline extends SparkApp[CreateTablesContext] {

  override def init(): CreateTablesContext = new CreateTablesContext()

  override def run(context: CreateTablesContext): Unit = {
    new CreateTablesLogic(session, config, context.force).run()
  }

}
