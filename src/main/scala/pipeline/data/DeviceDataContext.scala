package it.scarpenti.marioinc
package pipeline.data

import org.backuity.clist.arg

import java.util.Calendar


class DeviceDataContext() extends AbstractContext("device-data") {
  var receivedDate: Calendar = arg[Calendar](description = "Received date to process")
}

