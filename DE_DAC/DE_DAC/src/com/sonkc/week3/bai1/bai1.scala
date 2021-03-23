package com.sonkc.ex

import org.apache.spark.sql.SparkSession


object bai1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkByExamples")
      .master("local[1]")
      .getOrCreate()
    val sql = spark.sqlContext
    import sql.implicits._
    import org.apache.spark.sql.functions._
    val dates = Seq(
      "08/11/2015",
      "09/11/2015",
      "09/12/2015").toDF("date_string")

    dates.select("date_string", current_date().as("current_date")).show(false)

  }
}
