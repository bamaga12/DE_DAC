import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Week_3_Exercise_3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[1]").appName("SparkWeek3").getOrCreate()

    import spark.implicits._
    val salaries = spark
      .read
      .option("header", true)
      .option("inferSchema", true)
      .csv("salaries.csv")
    salaries.show(false)

    val solution = salaries.withColumn("Percentage", percent_rank over Window.orderBy(desc("Salary")))
      .withColumn("Percentage", when($"Percentage" < 0.4, "High").
                                        when($"Percentage" >= 0.4 and $"Percentage" < 0.5, "Average").
                                        otherwise("Low"))
    solution.show(false)
  }
}
