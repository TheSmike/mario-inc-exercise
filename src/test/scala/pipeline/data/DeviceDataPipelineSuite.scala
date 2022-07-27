package it.scarpenti.marioinc
package pipeline.data

import model.{Device, RawDevice}
import spark.SparkSessionFactory
import utils.DateUtils.toLocalDate

import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, LongType, StringType, TimestampType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers._
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper


class DeviceDataPipelineSuite extends AnyFunSuite with BeforeAndAfterAll {

  final val database = "tests_db"
  final val outputTableName = s"$database.test_duplicates_table"

  private val session = SparkSessionFactory.getSession("local")
  private val sc = session.sparkContext
  private val logic = new DeviceDataLogic(session, 1)

  import session.implicits._


  override def beforeAll(): Unit = {
    session.sql(s"CREATE DATABASE IF NOT EXISTS $database LOCATION '/tmp/marioinc/tests_db/' ")
    //FIXME in test env this command logs (but don't throws) the exception AlreadyExistsException
  }

  test("filterRawData should remove records older then 1 day") {
    val receivedDate = "2021-01-03"
    val correct1 = RawDevice(receivedDate, "8xUD6pzsQI", "2021-01-03T03:45:21.199Z", 917, 63, 27, "2021-01-03")
    val correct2 = RawDevice(receivedDate, "5gimpUrBB", "2021-01-02T01:35:50.749Z", 1425, 53, 15, "2021-01-02")
    val tooOld = RawDevice(receivedDate, "6al7RTAobR", "2021-01-01T21:13:44.839Z", 903, 72, 17, "2021-01-01")

    val input = sc.parallelize(List(correct1, correct2, tooOld)).toDF
    val expected = List(correct1, correct2)

    val result = logic.filterRawData(toLocalDate(receivedDate), input).as[RawDevice].collect().toList

    result should contain theSameElementsAs expected
  }

  test("filterRawData should remove duplicated records in the same input") {

    val JanFirst = "2021-01-01"
    val JanSecond = "2021-01-02"

    val id1 = "14QL93sBR0j"
    val id2 = "6al7RTAobR"
    val id3 = "2n2Pea"

    val ts1 = JanSecond + "T00:18:45.481Z"
    val ts2 = JanSecond + "T17:03:58.972Z"
    val ts3 = JanFirst + "T01:17:20.685Z"
    val ts4 = JanSecond + "T22:12:20.042Z"

    val input = sc.parallelize(List(
      RawDevice(JanSecond, id1, ts1, 828, 72, 10, JanSecond),
      RawDevice(JanSecond, id1, ts1, 828, 72, 10, JanSecond), //duplicated data
      RawDevice(JanSecond, id2, ts2, 1357, 25, 30, JanSecond),
      RawDevice(JanSecond, id3, ts3, 1005, 65, 20, JanFirst),
      RawDevice(JanSecond, id3, ts3, 1000, 60, 25, JanSecond), //duplicated data
      RawDevice(JanSecond, id3, ts4, 1581, 96, 21, JanSecond),
    )).toDF

    val result = logic.filterRawData(toLocalDate(JanSecond), input)
      .as[RawDevice].collect.toList
      .map(r => (r.device, r.timestamp.toString))

    result should have size 4
    result should contain allOf((id1, ts1), (id2, ts2), (id3, ts3), (id3, ts4))
  }

  test("multiple pipeline runs shouldn't load duplicated records") {
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

