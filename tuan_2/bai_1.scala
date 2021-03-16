import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions._

object Main {
  val spark = SparkSession.builder().master("local[1]").appName("SparkPractice").getOrCreate();
  import spark.implicits._
  val sqlContext: SQLContext = spark.sqlContext

  def main(args: Array[String]): Unit = {

    val nums = spark.range(5).withColumn("group", 'id%2)
    nums.show()

    val result = nums.groupBy("group").agg(max("id").alias("max_id"),min("id").alias("min_id"))
    result.show()

  }
}
