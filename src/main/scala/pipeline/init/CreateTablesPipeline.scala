package it.scarpenti.marioinc
package pipeline.init

import utils.spark.SparkApp

import org.apache.spark.sql.SparkSession

import scala.io.Source

object CreateTablesPipeline extends SparkApp[CreateTablesContext] {

  override def init(args: Array[String]): CreateTablesContext = new CreateTablesContext(args)

  override def run(session: SparkSession, context: CreateTablesContext): Unit = {
    // Note:  I usually prefer write files containing SQL statements to create
    // tables and run  them on an SQL Engine the first time in every environment,
    // if this is the official way to create tables, also the sql scripts should
    // be parametrized.

    session.sql(readQuery("create_database"))
    session.sql(readQuery("create_info_table"))
    session.sql(readQuery("create_raw_data_table"))
    //session.sql(readQuery("create_data_table"))
    //session.sql(readQuery("create_report_table"))
  }

  private def readQuery(fileName: String) = {
    //TODO fixme or delete me 
    Source.fromFile(s"ddl/$fileName.sql").getLines.mkString
  }

}
