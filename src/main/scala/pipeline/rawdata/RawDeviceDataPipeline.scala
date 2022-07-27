package it.scarpenti.marioinc
package pipeline.rawdata

object RawDeviceDataPipeline extends SparkApp[RawDeviceDataContext] {

  override def init(): RawDeviceDataContext = new RawDeviceDataContext()

  override def run(context: RawDeviceDataContext): Unit = {
    new RawDataLogic(session, config).run(context.receivedDate)
  }

}
