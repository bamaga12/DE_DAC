// spark 2.4.7
// scala 2.12.13

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat_ws}

object ConvertingArraysOfStringsToString {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[1]").appName("SparkPractice").getOrCreate()
    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")


    val words = Seq(Array("hello", "world")).toDF("words")
    //    words.show
    //    words.printSchema

    val solution = words.withColumn("solution", concat_ws(" ", $"words"))

    solution.show(false)
    solution.printSchema

  }
}