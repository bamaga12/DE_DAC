import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local")
      .appName("exercises")
      .config(new SparkConf())
      .getOrCreate()
    val sql = spark.sqlContext
    import sql.implicits._
    //exercises 2.1
    val nums = spark.range(5).withColumn("group", 'id % 2)
    val res = nums.groupBy($"group").agg(max("id").as("max_id"), min("id").as("min_id"))
    res.show()

    //exercises 2.2
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
    data.show()
    val tmp = data.groupBy($"id").pivot("day").agg(first("price").alias("price"), first("units").alias("unit"))
    val colName = Seq("id","price_1","unit_1","price_2","unit_2","price_3","unit_3","price_4","unit_4")
    val result = tmp.toDF(colName:_*)
    result.select("id", "price_1", "price_2", "price_3", "price_4", "unit_1", "unit_2", "unit_3", "unit_4").orderBy("id").show
  }
}
