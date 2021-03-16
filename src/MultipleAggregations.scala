// spark 2.4.7
// scala 2.12.13

import org.apache.spark.sql.{SparkSession}

import org.apache.spark.sql.functions.{min, max}

object MultipleAggregations {
  val spark = SparkSession.builder().master("local[1]").appName("SparkPractice").getOrCreate()
  import spark.implicits._

  def main(args: Array[String]): Unit = {

    val nums = spark.range(5).withColumn("group", 'id % 2)

//    nums.show

    val result = nums.groupBy("group").agg(max("id").alias("max_id"), min("id").alias("min_id"))
    result.show()

  }


}
