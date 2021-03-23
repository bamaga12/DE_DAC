package com.sonkc.tuan3.bai2

import org.apache.spark.sql.SparkSession

object bai2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkByExamples")
      .master("local[1]")
      .getOrCreate()
    val sql = spark.sqlContext
    import sql.implicits._
    import org.apache.spark.sql.functions._
    val words = Seq(Array("hello", "world")).toDF("words")
    val solution = words.withColumn("solution", concat_ws("",$"words"))
    solution.show(false)
    solution.printSchema()
  }
}
