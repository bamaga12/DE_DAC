// spark 2.4.7
// scala 2.12.13

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{desc, percent_rank, when}

object CalculatingPercentRank {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[1]").appName("SparkPractice").getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    val salaries = spark
      .read
      .option("header", true)
      .option("inferSchema", true)
      .csv("D:\\Users\\huyenpt36\\Desktop\\salaries.csv")
//    salaries.show

//    val tmp = salaries.withColumn("Percentage", percent_rank over Window.orderBy("Salary"))
//    tmp.show

    val result = salaries.withColumn("Percentage", percent_rank over Window.orderBy("Salary"))
      .withColumn("Percentage", when($"Percentage" > 0.7, "high").
        when($"Percentage" < 0.3, "low").
        otherwise("average")).orderBy(desc("Salary"))

    result.show

  }

}