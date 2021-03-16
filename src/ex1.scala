import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ex1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[1]").appName("SparkWeek2").getOrCreate()

    import spark.implicits._
    val nums = spark.range(5).withColumn("group", 'id % 2)
    nums.show(false)
    nums.groupBy("group").agg(max("id").alias("max_id"), min("id").alias("min_id")).orderBy("group").show(false)
  }
}
