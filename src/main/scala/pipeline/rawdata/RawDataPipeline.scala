package it.scarpenti.marioinc
package pipeline.rawdata

import utils.spark.SparkApp

import org.apache.spark.sql.{SaveMode, SparkSession}

object RawDataPipeline extends SparkApp[RawDataContext] {

  override def init(args: Array[String]): RawDataContext = new RawDataContext(args)

  override def run(session: SparkSession, context: RawDataContext): Unit = {
    val csv = session.read.json(context.inputPath)
    logger.info("schema is ==> " + csv.schema)

    csv
      .filter(csv("received") === context.receivedDate)
      .write
      .format("delta")
      .partitionBy("received")
      .mode(SaveMode.Overwrite)
      .option("replaceWhere", s"received = '${context.receivedDate}'")
      .save(context.outputPath)

    val deltaTable = session.read.format("delta").load(context.outputPath)
    println(s"deltaTable count 01 is => ${deltaTable.filter(deltaTable("received") === "2021-04-01").count()}")
    println(s"deltaTable count 02 is => ${deltaTable.filter(deltaTable("received") === "2021-04-02").count()}")

  }

}
