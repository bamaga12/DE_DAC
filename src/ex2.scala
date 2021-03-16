import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ex2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[1]").appName("SparkWeek2").getOrCreate()

    import spark.implicits._
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
    data.show(false)

    val priceCol = data.withColumn("priceCol", concat(lit("price_"), $"day")).groupBy("id").pivot("priceCol").agg(first("price"))

    val unitCol = data.withColumn("unitCol", concat(lit("unit_"), $"day")).groupBy("id").pivot("unitCol").agg(first("units"))
    priceCol.join(unitCol, Seq("id")).orderBy("id").show(false)
  }
}
