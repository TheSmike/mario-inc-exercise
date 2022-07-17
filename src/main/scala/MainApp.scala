package it.scarpenti.marioinc

import org.apache.spark.sql.SparkSession

object MainApp {

  def main(args: Array[String]) {

    println("START")

    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("Mario Inc. Assignment")
      .getOrCreate()

    val csv = spark.read.format("csv").
      option("delimiter", ",").
      option("header","true").
      load("src/test/resources/test_data/devices/")

    println(s"Count csv rows: ${csv.count()}")

    println(csv.select(csv("code")))

    csv.write.format("delta").save("target/tmp/devices")
    spark.read.format("delta").load("target/tmp/devices").show()

    println("END")
  }
}

