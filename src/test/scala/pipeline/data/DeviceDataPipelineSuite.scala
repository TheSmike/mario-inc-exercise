package it.scarpenti.marioinc
package pipeline.data

import model.RawData

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers._
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper


class DeviceDataPipelineSuite extends AnyFunSuite with DataFrameSuiteBase {

  test("cleanData should remove records older then 1 day") {
    import sqlContext.implicits._

    val receivedDate = "2021-04-02"
    val correct1 = RawData(917, "8xUD6pzsQI", 63, 27, "2021-04-01T03:45:21.199Z", receivedDate)
    val correct2 = RawData(1425, "5gimpUrBB", 53, 15, "2021-04-01T01:35:50.749Z", receivedDate)
    val tooOld = RawData(903, "6al7RTAobR", 72, 17, "2021-03-30T21:13:44.839Z", receivedDate)

    val input = sc.parallelize(List(tooOld, correct1, correct2)).toDF
    val expected = sc.parallelize(List(correct1, correct2)).toDF

    val result = DeviceDataPipeline.cleanData(receivedDate, input)

    assertDataFrameNoOrderEquals(result, expected)
  }

  test("cleanData should remove duplicated records") {
    import sqlContext.implicits._

    val receivedDate = "2021-04-04"

    val id1 = "14QL93sBR0j"
    val id2 = "6al7RTAobR"

    val id3 = "2n2Pea"
    val ts1 = receivedDate + "T00:18:45.481Z"
    val ts2 = receivedDate + "T17:03:58.972Z"
    val ts3 = "2021-04-03T01:17:20.685Z"
    val ts4 = receivedDate + "T22:12:20.042Z"

    val input = sc.parallelize(List(
      RawData(828, id1, 72, 10, ts1, receivedDate),
      RawData(828, id1, 72, 10, ts1, receivedDate), //duplicated data
      RawData(1357, id2, 25, 30, ts2, receivedDate),
      RawData(1005, id3, 65, 20, ts3, receivedDate),
      RawData(1005, id3, 65, 20, ts3, receivedDate), //duplicated data
      RawData(1581, id3, 96, 21, ts4, receivedDate),
    )).toDF

    val result = DeviceDataPipeline.cleanData(receivedDate, input)
      .as[RawData].collect.toList
      .map(r => (r.device, r.timestamp))

    result should have size 4
    result should contain allOf((id1, ts1), (id2, ts2), (id3, ts3), (id3, ts4))

  }

}

