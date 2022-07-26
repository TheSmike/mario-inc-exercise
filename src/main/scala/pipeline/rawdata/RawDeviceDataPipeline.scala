package it.scarpenti.marioinc
package pipeline.rawdata

import model.RawDevice

import org.apache.spark.sql.functions.{col, to_timestamp}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode}

object RawDeviceDataPipeline extends SparkApp[RawDeviceDataContext] {

  override def init(): RawDeviceDataContext = new RawDeviceDataContext()

  override def run(context: RawDeviceDataContext): Unit = {
    new RawDataLogic(session, config, context.force).run(context.receivedDate)
  }

}
