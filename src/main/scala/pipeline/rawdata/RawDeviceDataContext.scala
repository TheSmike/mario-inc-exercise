package it.scarpenti.marioinc
package pipeline.rawdata

import org.backuity.clist.arg

import java.util.Calendar

class RawDeviceDataContext() extends AbstractContext("device-raw-data") {
  var receivedDate: Calendar = arg[Calendar](description = "Received date to process")
}

