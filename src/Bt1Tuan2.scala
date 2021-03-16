import org.apache.spark.sql.SparkSession

object Bt1Tuan2 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkExamples")
      .master("local")
      .getOrCreate()
    val sql = spark.sqlContext
    import sql.implicits._
    val nums = spark.range(5).withColumn("group", 'id % 2)

    import org.apache.spark.sql.functions._
    val result = nums.groupBy("group").agg(max("id").as("max_id"), min("id").as("min_id"))
    result.show()
  }
}