package it.scarpenti.marioinc
package pipeline.data

import model.RawDevice
import pipeline.TestUtils.initTestAppConfig
import utils.DateUtils.toLocalDate

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers._
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper


class DeviceDataPipelineSuite extends AnyFunSuite with DataFrameSuiteBase {

  val config = initTestAppConfig(maxDelay = 1)
  val logic = new DeviceDataLogic(spark, config)

  test("filterRawData should remove records older then 1 day") {
    import sqlContext.implicits._

    val receivedDate = "2021-01-03"
    val correct1 = RawDevice(receivedDate, "8xUD6pzsQI", "2021-01-03T03:45:21.199Z", 917, 63, 27, "2021-01-03")
    val correct2 = RawDevice(receivedDate, "5gimpUrBB", "2021-01-02T01:35:50.749Z", 1425, 53, 15, "2021-01-02")
    val tooOld = RawDevice(receivedDate, "6al7RTAobR", "2021-01-01T21:13:44.839Z", 903, 72, 17, "2021-01-01")

    val input = sc.parallelize(List(correct1, correct2, tooOld)).toDF
    val expected = sc.parallelize(List(correct1, correct2)).toDF

    val result = logic.filterRawData(toLocalDate(receivedDate), input)

    assertDataFrameNoOrderEquals(result, expected)
  }

  test("filterRawData should remove duplicated records in the same input") {
    import sqlContext.implicits._

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

}

