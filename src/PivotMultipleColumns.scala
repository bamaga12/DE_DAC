import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions._

object PivotMultipleColumns {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[1]").getOrCreate();
    //   bai tap 2 pivot
    import spark.implicits._

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
      (102, 4, 78, 18)).toDF("id", "day", "price", "units");

    val df_price = data.withColumn("abc",concat(lit("price_"),col("day"))).groupBy("id").pivot("abc").agg(sum("price"));

    val df_units = data.withColumn("abc",concat(lit("units_"),col("day"))).groupBy("id").pivot("abc").agg(sum("units"));

    df_price.join(df_units,Seq("id"),"inner").orderBy("id").show(false);

  }
}