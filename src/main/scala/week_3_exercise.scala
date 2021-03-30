package main.scala

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.functions._

object week_3_exercise {
  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("practice spark")
    .getOrCreate();

  import spark.implicits._

  spark.sparkContext.setLogLevel("ERROR")

  def main(args: Array[String]): Unit = {
    // Exercise 1
    val dates = Seq(
      "08/11/2015",
      "09/11/2015",
      "09/12/2015").toDF("date_string")
    val solution_1 = dates.withColumn("to_date", to_date(col("date_string"), "MM/dd/yyyy"))
      .withColumn("diff", datediff(current_date(), col("to_date")))
    solution_1.show()

    // Exercise 2
    val words = Seq(Array("hello", "world")).toDF("words")
    words.show
    val solution_2 = words.withColumn("solution", concat_ws(" ", col("words")))
    solution_2.show()

    // Exercise 3
    val salaries = spark
      .read
      .option("header", true)
      .option("inferSchema", true)
      .csv("resource/salaries.csv")
    salaries.show()
    val bySalary = Window.orderBy(col("Salary").desc)
    val solution_3 = salaries.withColumn("Percentage", percent_rank() over bySalary)
      .withColumn("Percentage", when(col("Percentage") <= 0.3, "High").
        when(col("Percentage") > 0.3 && col("Percentage") <= 0.7, "Average").
        otherwise("Low"))
    solution_3.show()
  }
}
