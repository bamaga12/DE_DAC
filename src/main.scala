import breeze.numerics.constants.c
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object main {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Simple Application").config("spark.master", "local[*]").getOrCreate()
    import spark.implicits._
    //////////////Q1
    val nums = spark.range(5).withColumn("group", 'id % 2)
    nums.show(false)
    nums.groupBy("group").agg(max("id").alias("max_id"),min("id").alias("min_id")).show(false)
    //////////////Q2
    val data = Seq(
      (100,1,23,10),
      (100,2,45,11),
      (100,3,67,12),
      (100,4,78,13),
      (101,1,23,10),
      (101,2,45,13),
      (101,3,67,14),
      (101,4,78,15),
      (102,1,23,10),
      (102,2,45,11),
      (102,3,67,16),
      (102,4,78,18)).toDF("id", "day", "price", "units")

    val result2 = data.groupBy("id").pivot("day").agg(first("price").alias("price"), first("units").alias("unit") )
    result2.show(false)
  }
}
