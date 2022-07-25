package it.scarpenti.marioinc
package pipeline.data

import model.RawDevice

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers._
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper


class DeviceDataPipelineSuite extends AnyFunSuite with DataFrameSuiteBase {


//  TODO reimplement them!
//  test("cleanData should remove records older then 1 day") {
//    import sqlContext.implicits._
//
//    val receivedDate = "2021-04-02"
//    val correct1 = RawDevice(receivedDate, "8xUD6pzsQI", "2021-04-01T03:45:21.199Z", 917, 63, 27, "2021-01-01")
//    val correct2 = RawDevice(receivedDate, "5gimpUrBB", "2021-04-01T01:35:50.749Z", 1425, 53, 15, "2021-01-01")
//    val tooOld = RawDevice(receivedDate, "6al7RTAobR", "2021-03-30T21:13:44.839Z", 903, 72, 17, "2021-01-01")
//
//    val input = sc.parallelize(List(tooOld, correct1, correct2)).toDF
//    val expected = sc.parallelize(List(correct1, correct2)).toDF
//
//    val result = DeviceDataPipeline.filterData(receivedDate, input)
//
//    assertDataFrameNoOrderEquals(result, expected)
//  }
//
//  test("cleanData should remove duplicated records") {
//    import sqlContext.implicits._
//
//    val receivedDate = "2021-04-04"
//
//    val id1 = "14QL93sBR0j"
//    val id2 = "6al7RTAobR"
//
//    val id3 = "2n2Pea"
//    val ts1 = receivedDate + "T00:18:45.481Z"
//    val ts2 = receivedDate + "T17:03:58.972Z"
//    val ts3 = "2021-04-03T01:17:20.685Z"
//    val ts4 = receivedDate + "T22:12:20.042Z"
//
//    val input = sc.parallelize(List(
//      RawDevice(receivedDate, id1, ts1, 828, 72, 10, "2021-01-01"),
//      RawDevice(receivedDate, id1, ts1, 828, 72, 10, "2021-01-01"), //duplicated data
//      RawDevice(receivedDate, id2, ts2, 1357, 25, 30, "2021-01-01"),
//      RawDevice(receivedDate, id3, ts3, 1005, 65, 20, "2021-01-01"),
//      RawDevice(receivedDate, id3, ts3, 1005, 65, 20, "2021-01-01"), //duplicated data
//      RawDevice(receivedDate, id3, ts4, 1581, 96, 21, "2021-01-01"),
//    )).toDF
//
//    val result = DeviceDataPipeline.filterData(receivedDate, input)
//      .as[RawDevice].collect.toList
//      .map(r => (r.device, r.timestamp))
//
//    result should have size 4
//    result should contain allOf((id1, ts1), (id2, ts2), (id3, ts3), (id3, ts4))
//
//  }

}

