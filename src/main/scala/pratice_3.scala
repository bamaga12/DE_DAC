package main.scala

import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.functions._

object practice_4 {
  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("practice spark")
    .getOrCreate();

  import spark.implicits._

  spark.sparkContext.setLogLevel("ERROR")

  def main(args: Array[String]): Unit = {
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
    val pricePivoted = data.groupBy("id")
      .pivot("day")
      .agg(sum("price"))
      .toDF("id", "price_1", "price_2", "price_3", "price_4")
    val unitsPivoted = data.groupBy("id")
      .pivot("day")
      .agg(sum("units"))
      .toDF("id", "units_1", "units_2", "units_3", "units_4")
    val solution = pricePivoted.as("p").join(unitsPivoted.as("u"), pricePivoted("id") === unitsPivoted("id"), "inner")
      .select("p.*", "u.units_1", "u.units_2", "u.units_3", "u.units_4").orderBy("p.id")
    solution.show()
  }
}
