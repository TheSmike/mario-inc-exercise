package it.scarpenti.marioinc
package pipeline.rawdata

import utils.spark.SparkApp

import org.apache.spark.sql.{SaveMode, SparkSession}

object RawDeviceDataPipeline extends SparkApp[RawDeviceDataContext] {

  override def init(args: Array[String]): RawDeviceDataContext = new RawDeviceDataContext(args)

  override def run(session: SparkSession, context: RawDeviceDataContext): Unit = {
    val csv = session.read.json(context.inputPath)
    logger.debug("schema is ==> " + csv.schema)

    val filtered = csv
      .filter(csv("received") === context.receivedDate)

    filtered
      .write
      .format("delta")
      .partitionBy("received")
      .mode(SaveMode.Overwrite)
      .option("replaceWhere", s"received = '${context.receivedDate}'")
      .saveAsTable(context.fullTableName)

  }


}
