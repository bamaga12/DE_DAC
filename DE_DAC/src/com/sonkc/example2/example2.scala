package com.sonkc.example2

import org.apache.spark.sql.SparkSession

object example2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkByExamples")
      .master("local[1]")
      .getOrCreate()
    val sql = spark.sqlContext
    import sql.implicits._
    import org.apache.spark.sql.functions._
    val data = Seq(
      (100, 1, 23, 10),
      (100, 2, 45, 11),
      (100, 3, 67, 12),
      (100, 4, 78, 13),
      (101, 1, 23, 10),
      (101, 2, 45, 13),
      (101, 3, 67, 14),
      (101, 4, 78, 15),
      (102, 1, 23, 10),
      (102, 2, 45, 11),
      (102, 3, 67, 16),
      (102, 4, 78, 18)).toDF("id", "day", "price", "units")
    data.show(false)
    val tmpData = data.groupBy("id").pivot("day").agg(first("price").alias("price"), first("units").alias("unit"))
    val col = Seq("id","price_1","unit_1","price_2","unit_2","price_3","unit_3","price_4","unit_4")
    val result = tmpData.toDF(col:_*)
    result.select("id", "price_1", "price_2", "price_3", "price_4", "unit_1", "unit_2", "unit_3", "unit_4").orderBy("id").show(false)

  }
}
