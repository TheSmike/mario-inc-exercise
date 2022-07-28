package it.scarpenti.marioinc
package pipeline.data

import config.AppConfig
import model.{Device, RawDevice}
import pipeline.TestUtils.initTestAppConfig
import pipeline.init.CreateTablesLogic
import spark.SparkSessionFactory
import utils.DateUtils.toLocalDate
import utils.Profiles.LOCAL

import io.delta.tables.DeltaTable
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers._
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper


class DeviceDataPipelineSuite extends AnyFunSuite {

  private val session = SparkSessionFactory.getSession(LOCAL)
  private val sc = session.sparkContext

  import session.implicits._

  test("filterRawData should remove records with event date not in the range (eventDateFrom, eventDateTo)") {
    val receivedDate = "2021-01-03"
    val correct1 = RawDevice(receivedDate, "8xUD6pzsQI", "2021-01-03T03:45:21.199Z", 917, 63, 27, "2021-01-03")
    val correct2 = RawDevice(receivedDate, "5gimpUrBB", "2021-01-02T01:35:50.749Z", 1425, 53, 15, "2021-01-02")
    val tooOld = RawDevice(receivedDate, "6al7RTAobR", "2021-01-01T21:13:44.839Z", 903, 72, 17, "2021-01-01")

    val input = sc.parallelize(List(correct1, correct2, tooOld)).toDF
    val expected = List(correct1, correct2)

    val logic = new DeviceDataLogic(session, initTestAppConfig(), LOCAL)
    val eventDateFrom = toLocalDate("2021-01-02")
    val eventDateTo = toLocalDate("2021-01-03")
    val result = logic.filterRawData(input, eventDateFrom, eventDateTo).as[RawDevice].collect().toList

    result should contain theSameElementsAs expected
  }

  test("filterRawData should remove duplicated records") {

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

    val logic = new DeviceDataLogic(session, initTestAppConfig(), LOCAL)

    val eventDateFrom = toLocalDate(JanFirst)
    val eventDateTo = toLocalDate(JanSecond)
    val result = logic.filterRawData(input, eventDateFrom, eventDateTo).as[RawDevice].collect.toList
      .map(r => (r.device, r.timestamp.toString))

    result should have size 4
    result should contain allOf((id1, ts1), (id2, ts2), (id3, ts3), (id3, ts4))
  }

  test("multiple pipeline runs shouldn't load duplicated records") {

    val database = "tests_db"
    val rawDataTableName = s"raw_duplicates_on_different_dates"
    val outputTableName = s"$database.test_duplicates_table"

    val config = initTestAppConfig(
      database = database,
      rawDataTableName = rawDataTableName,
      dataTableName = outputTableName,
    )

    initRawData(rawDataTableName)
    initOutputTable(outputTableName, config)

    val logic = new DeviceDataLogic(session, config, LOCAL)

    logic.run(toLocalDate("2021-04-01"))
    logic.run(toLocalDate("2021-04-02"))

    val result = session.read.table(outputTableName).as[Device].collect.toList
      .map(r => (r.device, r.event_timestamp))

    result should contain theSameElementsAs result.toSet
  }

  private def initOutputTable(outputTableName: String, config: AppConfig): Unit = {
    val createLogic = new CreateTablesLogic(session, config)
    createLogic.createDataTable(true)
    DeltaTable.forName(outputTableName).delete()
  }

  private def initRawData(rawDataTableName: String): Unit = {
    val rawInputDs = session.read.schema(rawSchema).json("src/test/resources/test_data/duplicates_on_different_dates")
    rawInputDs.createOrReplaceTempView(rawDataTableName)
  }

  private val rawSchema = StructType(Array(
    StructField(RawDevice.RECEIVED, DateType),
    StructField(RawDevice.DEVICE, StringType),
    StructField(RawDevice.TIMESTAMP, TimestampType),
    StructField(RawDevice.CO2_LEVEL, LongType),
    StructField(RawDevice.HUMIDITY, LongType),
    StructField(RawDevice.TEMPERATURE, LongType),
    StructField(RawDevice.EVENT_DATE, DateType),
  ))

}

