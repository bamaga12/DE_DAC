import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object main {
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

    spark.range(5).withColumn("group", 'id % 2)
    // Exercise 2
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

   val pivot_df_1 = data
      .groupBy("id")
      .pivot(col("day"))
      .avg("price")

    val pivot_df_2 = data
      .groupBy("id")
      .pivot(col("day"))
      .avg("units")

    val pivot_df_1_rename = pivot_df_1.select(pivot_df_1.columns.map(c => if(c == "id") col(c) else col(c).alias("price_"+c)): _*)
    val pivot_df_2_rename = pivot_df_2.select(pivot_df_2.columns.map(c => if(c == "id") col(c) else col(c).alias("units_"+c)): _*)
    val result2 = pivot_df_1_rename.join(pivot_df_2_rename,Seq("id"),"inner").orderBy(asc("id"))

    result_1.show(false)

    result2.show(false)

  }
}
