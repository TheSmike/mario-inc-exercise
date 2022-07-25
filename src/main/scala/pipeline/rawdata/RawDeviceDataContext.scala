package it.scarpenti.marioinc
package pipeline.rawdata

import org.backuity.clist.arg

class RawDeviceDataContext() extends AbstractContext("device-raw-data") {
  var receivedDate: String = arg[String](description = "Received date to process in the form yyyy-MM-dd. i.e.: 2021-01-01")
}

