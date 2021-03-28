import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, concat_ws, current_date, datediff, desc, percent_rank, rank, to_date, when}

object Week_3_Exercise_3 {
  //  spark: 2.4.7
  //  scala: 2.12.10
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("thaolt34")
      .getOrCreate()

    import spark.sqlContext.implicits._

    val salaries = spark
      .read
      .option("header", true)
      .option("inferSchema", true)
      .csv("./input_week_3_exercise_3.csv")

    salaries.show

    val window = Window.orderBy($"salary".desc)

    val result = salaries
      .withColumn("Percentage", percent_rank().over(window))
      .withColumn("Percentage",
        when($"Percentage" < 0.3, "High")
        when($"Percentage" > 0.7, "Low")
        otherwise("Average")
      )
    result.show

  }
}
