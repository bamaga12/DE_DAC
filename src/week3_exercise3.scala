import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object week3_exercise3 {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .getOrCreate()

    val salaries = spark
      .read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv("data/salaries.csv")

    salaries.show(false)

    val windowSpec  = Window.orderBy(desc("Salary"))

    salaries.withColumn("Percentage",percent_rank().over(windowSpec))
      .withColumn("Percentage", when(col("Percentage") < 0.3, "High").
                                          when(col("Percentage") < 0.7, "Average").
                                          otherwise("Low"))
      .show()

  }

}
