package tuan2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("thaolt34")
      .getOrCreate()

    import spark.sqlContext.implicits._
    // Question 1

    val nums = spark.range(5).withColumn("group", 'id % 2)
    nums.show
    val result = nums.groupBy("group")
      .agg(max("id") as "max_id", min("id") as "min_id")
      .orderBy("group")
    result.show

    //Question 2
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

    val df1 = data.groupBy("id")
      .pivot("price")
      .agg(first("price"))
      .toDF("id","price_1", "price_2", "price_3", "price_4")
    df1.show
    val df2 = data.groupBy("id")
      .pivot("price")
      .agg(first("units"))
      .toDF("id","unit_1", "unit_2", "unit_3", "unit_4")

    val result1 = df1.join(df2, "id").orderBy("id")
    result1.show
  }
}

