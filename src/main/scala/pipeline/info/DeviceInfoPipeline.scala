package it.scarpenti.marioinc
package pipeline.info

object DeviceInfoPipeline extends SparkApp[DeviceInfoContext] {

  override def init(): DeviceInfoContext = new DeviceInfoContext()

  override def run(context: DeviceInfoContext): Unit = {
    new DeviceInfoLogic(session, config).run()
  }

}
