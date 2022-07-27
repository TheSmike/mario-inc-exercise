package it.scarpenti.marioinc
package pipeline.data

import org.backuity.clist.arg


class DeviceDataContext() extends AbstractContext("device-data") {
  var receivedDate: String = arg[String](description = "Received date to process in the form yyyy-MM-dd. i.e.: 2021-01-01")
}

