package com.sonkc.example1

import org.apache.spark.sql.SparkSession

object example1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkExamples")
      .master("local")
      .getOrCreate()
    val sql = spark.sqlContext
    import sql.implicits._
    val nums = spark.range(5).withColumn("group", 'id % 2)
    //val res = nums.groupBy($"id").agg(sum($"group"))
    //val res = nums.groupBy($"group").agg(max("id").as("max_id"), min("id").as("min_id"))
    import org.apache.spark.sql.functions._
    nums.groupBy("group")
      .agg(max("id").as("max_id"),
        min("id").as("min_id"))
      .show(false)
  }
}
