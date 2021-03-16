import org.apache.spark.sql.SparkSession

object Bt2Tuan2 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkExamples")
      .master("local")
      .getOrCreate()
    val sql = spark.sqlContext
    import sql.implicits._
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

    import org.apache.spark.sql.functions._
    val r1 = data.groupBy("id").pivot("day").agg(sum("price").as("price"), sum("units").as("units"))
    val r2 = r1.select($"id", $"1_price".as("price_1"), $"2_price".as("price_2"), $"3_price".as("price_3"), $"4_price".as("price_4"),
      $"1_units".as("units_1"), $"2_units".as("units_2"), $"3_units".as("units_3"), $"4_units".as("units_4")).orderBy("id")
    r2.show()

  }
}