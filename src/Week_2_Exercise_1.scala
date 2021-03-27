import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Week_2_Exercise_1 {
  def main(args: Array[String]): Unit = {
    // create spark session
    val spark = SparkSession
      .builder()
      .appName("anhvt105")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._


    //Exercise 1
    val nums = spark.range(5).withColumn("group", 'id % 2)
    val result_1 = nums.groupBy("group").agg(
      max(col("id")).alias("max_id"),
      min(col("id")).alias("min_id")
    ).orderBy(asc("group"))

    result_1.show(false)
  }
}
