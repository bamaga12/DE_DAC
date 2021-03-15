package main.scala

import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.functions._

object practice_2 {
  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("practice spark")
    .getOrCreate();

  import spark.implicits._

  spark.sparkContext.setLogLevel("ERROR")

  def main(args: Array[String]): Unit = {
    val nums = spark.range(5).withColumn("group", 'id % 2)
    nums.show()
    val solution = nums.groupBy("group").agg(
      max("id").as("max_id"),
      min("id").as("min_id")).orderBy("group")
    solution.show(truncate = false)
  }
}
