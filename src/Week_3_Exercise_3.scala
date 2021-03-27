import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{asc, col, percent_rank, when}
import org.apache.spark.sql.expressions.Window

object Week_3_Exercise_3 {
  def main(args: Array[String]): Unit = {
    // create spark session
    val spark = SparkSession
      .builder()
      .appName("anhvt105")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val salaries = spark
      .read
      .option("header", true)
      .option("inferSchema", true)
      .csv("../DE_DAC/src/salaries.csv")

    val window = Window.partitionBy().orderBy(salaries("Salary"))

    val result = salaries
      .withColumn("rank",percent_rank().over(window))
      .withColumn("Percentage", when(col("rank") <= 0.3,"High" )
        .when(col("rank") <= 0.7 && col("rank") > 0.3,"Average")
        .otherwise("Low"))
      .orderBy("rank")
      .drop("rank")

    result.show()
  }
}
