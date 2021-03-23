package com.sonkc.tuan3.bai3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window

object bai3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkByExamples")
      .master("local[1]")
      .getOrCreate()
    val sql = spark.sqlContext
    import sql.implicits._
    import org.apache.spark.sql.functions._
    val data = Seq(
      ("Tony", 50),
      ("Alan", 45),
      ("Lee", 60),
      ("David", 35),
      ("Steve", 65),
      ("Paul", 48),
      ("Micky", 62),
      ("George", 80),
      ("Nigel", 64),
      ("John", 42)).toDF("Employee","Salary")
    data.show(false)
    val temp = data.withColumn("Percentage", percent_rank over Window.orderBy('Salary.desc))
    val rs = temp.withColumn("Percentage", when($"Percentage" < 0.4, "High").
      when($"Percentage" > 0.5, "Low").
      otherwise("Average"))
    rs.show(false)
  }
}
