package it.scarpenti.marioinc
package pipeline.init

import utils.spark.SparkApp

import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DateType

import scala.io.{BufferedSource, Source}

/**
 * This pipeline is used to create data tables in the data catalog the first time.
 * This operation could also be done by launching SQL scripts directly into SQL Engine system.
 * I usually prefer write files containing SQL statements to create tables and run them on
 * an SQL Engine the first time in every environment, if this is the official way to create tables,
 * also the sql scripts should be parametrized.
 */
object CreateTablesPipeline extends SparkApp[CreateTablesContext] {

  override def init(args: Array[String]): CreateTablesContext = new CreateTablesContext(args)

  override def run(session: SparkSession, context: CreateTablesContext): Unit = {

    session.sql(readQuery("create_database"))
    session.sql(readQuery("create_info_table"))
    session.sql(readQuery("create_raw_data_table"))
    session.sql(readQuery("create_report_table"))

    createDataTable(context)
    //session.sql(readQuery("create_report_table"))
  }
  private def readQuery(fileName: String) = {
    val source: BufferedSource = Source.fromFile(s"ddl/$fileName.sql")
    val sql = source.getLines.mkString(sep = "\n")
    source.close()
    sql
  }

  def createDataTable(context: CreateTablesContext): Unit = {

    DeltaTable.createIfNotExists(session)
      .tableName(context.dataTableName)
      //TODO add comments if there is no other data catalog
      .addColumn("received_date", "DATE")
      .addColumn("event_timestamp", "TIMESTAMP")
      .addColumn("device", "STRING")
      .addColumn("CO2_level", "BIGINT")
      .addColumn("humidity", "BIGINT")
      .addColumn("temperature","BIGINT")
      .addColumn(
        DeltaTable.columnBuilder("event_date")
          .dataType(DateType)
          .generatedAlwaysAs("CAST(event_timestamp AS DATE)")
          .build())
      .partitionedBy("event_date", "device")
      .location(context.dataPath)
      .execute()
  }

}
