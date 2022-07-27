package it.scarpenti.marioinc
package model

case class Info(
                 code: String,
                 `type`: String,
                 area: String,
                 customer: String,
               )

object Info {
  final val CODE = "code"
  final val TYPE = "type"
  final val AREA = "area"
  final val CUSTOMER = "customer"
}