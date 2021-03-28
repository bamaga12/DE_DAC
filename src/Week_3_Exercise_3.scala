import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{percent_rank, when}

object Week_3_Exercise_3 {
  //Spark 3.1.1 - SCALA 2.12
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Simple Application").config("spark.master", "local[*]").getOrCreate()
    import spark.implicits._

    val salaries = spark
      .read
      .option("header", true)
      .option("inferSchema", true)
      .csv("D:\\MyRepo\\DE_DAC\\data\\salaries.csv")

    val df2 = salaries.withColumn("Percentage", percent_rank over Window.orderBy("Salary"))
      .withColumn("Percentage", when($"Percentage" > 0.7, "High").
        when($"Percentage" > 0.6, "Average").
        otherwise("Low")).orderBy($"Salary".desc)
    df2.show(false)
  }
}
