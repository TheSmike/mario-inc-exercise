package it.scarpenti.marioinc
package pipeline.data

import model.Device
import spark.SparkSessionFactory
import utils.DateUtils.toLocalDate

import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, LongType, StringType, TimestampType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper


class DeviceDataPipelineITSuite extends AnyFunSuite with BeforeAndAfterAll {

  final val database = "tests_db"
  final val outputTableName = s"$database.test_duplicates_table"

  private val session = SparkSessionFactory.getSession("local")

  override def beforeAll(): Unit = {
    session.sql(s"CREATE DATABASE IF NOT EXISTS $database LOCATION '/tmp/marioinc/tests_db/' ")
    //FIXME in test env this command logs (but don't throws) the exception AlreadyExistsException
  }

  test("multiple pipeline runs shouldn't load duplicated records") {
    import session.implicits._
    val logic = new DeviceDataLogic(session, 1)

    val rawInputDs = session.read.json("src/test/resources/test_data/duplicates_on_different_dates")
    val outputDeltaTable = createTmpDataDeltaTable(session, outputTableName)

    logic.run(toLocalDate("2021-04-01"), rawInputDs, outputDeltaTable)
    logic.run(toLocalDate("2021-04-02"), rawInputDs, outputDeltaTable)

    val result = session.read.table(outputTableName).as[Device].collect.toList
      .map(r => (r.device, r.event_timestamp))

    result should contain theSameElementsAs result.toSet
  }

  private def createTmpDataDeltaTable(session: SparkSession, tableName: String): DeltaTable = {
    if (DeltaTable.isDeltaTable(tableName))
      DeltaTable.forName(tableName).delete()

    DeltaTable.createOrReplace(session)
      .tableName(tableName)
      .addColumn(Device.RECEIVED_DATE, DateType)
      .addColumn(Device.EVENT_TIMESTAMP, TimestampType)
      .addColumn(Device.DEVICE, StringType)
      .addColumn(Device.CO2_LEVEL, LongType)
      .addColumn(Device.HUMIDITY, LongType)
      .addColumn(Device.TEMPERATURE, LongType)
      .addColumn(
        DeltaTable.columnBuilder(Device.EVENT_DATE)
          .dataType(DateType)
          .generatedAlwaysAs(s"CAST(${Device.EVENT_TIMESTAMP} AS DATE)")
          .build())
      .partitionedBy(Device.EVENT_DATE, Device.DEVICE)
      .location(s"/tmp/marioinc/${tableName.replace('.', '/')}/")
      .execute()
    //TODO part of this method could be refactored to match the create table in the init pipeline
  }

}

