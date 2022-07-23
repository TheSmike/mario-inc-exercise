package it.scarpenti.marioinc
package config

case class AppConfig(
                      database: String,
                      databasePath: String,

                      infoLandingZonePath: String,
                      infoTableName: String,
                      infoOutputPath: String,

                      rawDataLandingZonePath: String,
                      rawDataTableName: String,
                      rawOutputPath: String,

                      dataTableName: String,
                      dataOutputPath: String,

                      reportTableName: String,
                      reportOutputPath: String,
                    )
