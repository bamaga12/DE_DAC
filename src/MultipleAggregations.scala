import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions._

object MultipleAggregations {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate();
    val nums = spark.range(5).withColumn("group", col("id") % 2);

    //    bai tap 1 multiple aggregate
    val df = nums.groupBy("group").agg(max("id").as("max_id"), min(("id")).as("min_id"));
    df.show();
  }
}