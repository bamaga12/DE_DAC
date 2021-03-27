package Week2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Ex1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Exercise_1")
      .master("local")
      .getOrCreate()
    val nums = spark.range(5).withColumn("group", col("id") % 2)

    nums.show(false)
    val result = nums.groupBy(col("group"))
      .agg(max("id").as("max_id"),min("id").as("min_id"))

    result.show(false)
  }
}
