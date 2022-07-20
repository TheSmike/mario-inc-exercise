package it.scarpenti.marioinc
package pipeline.info

import utils.spark.SparkApp

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{SaveMode, SparkSession}

  object DeviceInfoPipeline extends SparkApp[DeviceInfoContext] {

    override def init(args: Array[String]): DeviceInfoContext = new DeviceInfoContext(args)

  override def run(session: SparkSession, context: DeviceInfoContext): Unit = {
    val csv = session.read.format("csv").option("header", "true").load(context.inputPath)
    logger.info("schema is: " + csv.schema)

    //csv.withColumn("processing_date", lit(context.processingDate))
    //note: the processing_date column could be useful if we store every snapshot of the device table.
    // if we replace the entire datum each week, like in this case, it is not needful.

    csv.write.format("delta").mode(SaveMode.Overwrite).save(context.outputPath)
  }

}
