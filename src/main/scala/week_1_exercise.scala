package main.scala

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions._

object week_1_exercise {
  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("practice spark")
    .getOrCreate();

  import spark.implicits._

  spark.sparkContext.setLogLevel("ERROR")

  def main(args: Array[String]): Unit = {
    val dept = Seq(("50000.0&0&0&", "&"), ("0@1000.0@", "@"), ("1$", "$"), ("1000.00^Test_string", "^"))
      .toDF("VALUES", "Delimiter");

    // Make the delimiter string
    val delimiterArray = dept.select("Delimiter").collect.map(_.toSeq)
    val delimiterString = delimiterArray.flatten.mkString("")

    // Add new columns
    val df = dept.withColumn("split_values", split(col("VALUES"), "[" + delimiterString + "]"))
      .withColumn("extra", array_remove(split(col("VALUES"), "[" + delimiterString + "]"), ""))
    df.printSchema()
    df.show()

    val input = Seq(
      ("100", "John", Some(35), None),
      ("100", "John", None, Some("Georgia")),
      ("101", "Mike", Some(25), None),
      ("101", "Mike", None, Some("New York")),
      ("103", "Mary", Some(22), None),
      ("103", "Mary", None, Some("Texas")),
      ("104", "Smith", Some(25), None),
      ("105", "Jake", None, Some("Florida"))).toDF("id", "name", "age", "city")
    input.show()
    val solution = input.groupBy("id", "name").agg(
      max("age").as("age"),
      first("city", true).as("city")).orderBy("id")
    solution.show(truncate = false)
  }
}
