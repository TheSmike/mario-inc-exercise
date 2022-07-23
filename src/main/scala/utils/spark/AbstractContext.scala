package it.scarpenti.marioinc
package utils.spark

import org.backuity.clist.{Command, arg, opt}

class AbstractContext(name: String) extends Command(name) {
  var profile: String = arg[String](description = "The profile corresponding to the environment in which the pipeline runs. i.e.: local, staging, production")
  var force: Boolean = opt[Boolean](description = "tables will be replaced in force mode", default = false, abbrev = "f")
}
