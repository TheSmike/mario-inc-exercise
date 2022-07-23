package it.scarpenti.marioinc
package pipeline.rawdata

import org.apache.spark.sql.SaveMode

object RawDeviceDataPipeline extends SparkApp[RawDeviceDataContext] {

  override def init(): RawDeviceDataContext = new RawDeviceDataContext()

  override def run(context: RawDeviceDataContext): Unit = {
    val csv = session.read.json(config.rawDataLandingZonePath)
    logger.debug("schema is ==> " + csv.schema)

    val filtered = csv
      .filter(csv("received") === context.receivedDate)

    filtered
      .write
      .format("delta")
      .partitionBy("received")
      .mode(SaveMode.Overwrite)
      .option("replaceWhere", s"received = '${context.receivedDate}'")
      .saveAsTable(config.rawDataTableName)

  }


}
