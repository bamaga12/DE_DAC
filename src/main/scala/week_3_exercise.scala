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
      .sum("price")
    val pricePivotedRename = pricePivoted.select(pricePivoted.columns.map(i => if (i != "id") col(i).as("price_" + i) else col(i)): _*)
    val unitsPivoted = data.groupBy("id")
      .pivot("day")
      .agg(sum("units"))
    val unitsPivotedRename = unitsPivoted.select(unitsPivoted.columns.map(i => if (i != "id") col(i).as("unit_" + i) else col(i)): _*)
    val solution = pricePivotedRename.as("p").join(unitsPivotedRename.as("u"), Seq("id"), "inner")
      .select("p.*", "u.*").orderBy($"id".asc)
    solution.show()
  }
}
