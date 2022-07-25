package it.scarpenti.marioinc
package pipeline.data

import utils.DateUtils.toLocalDate

object DeviceDataPipeline extends SparkApp[DeviceDataContext] {

  override def init(): DeviceDataContext = new DeviceDataContext()

  override def run(context: DeviceDataContext): Unit = {
    val receivedDate = toLocalDate(context.receivedDate)
    new DeviceDataLogic(session, config, context.force).run(receivedDate)
  }

}

