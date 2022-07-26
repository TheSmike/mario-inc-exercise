package it.scarpenti.marioinc
package pipeline

import config.AppConfig

object TestUtils {

  def initTestAppConfig(
                        database: String = null,
                        databasePath: String = null,

                        infoLandingZonePath: String = null,
                        infoTableName: String = null,
                        infoOutputPath: String = null,

                        rawDataLandingZonePath: String = null,
                        rawDataTableName: String = null,
                        rawOutputPath: String = null,

                        dataTableName: String = null,
                        dataOutputPath: String = null,
                        maxDelay: Int = 1,

                        reportTableName: String = null,
                        reportOutputPath: String = null,
                      ) = AppConfig(database, databasePath, infoLandingZonePath, infoTableName, infoOutputPath, rawDataLandingZonePath, rawDataTableName, rawOutputPath, dataTableName, dataOutputPath, maxDelay, reportTableName, reportOutputPath)
}
