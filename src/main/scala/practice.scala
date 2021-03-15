package main.scala

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions._

object practice {
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
  }
}
