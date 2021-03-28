import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object week3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local")
      .appName("week3")
      .config(new SparkConf())
      .getOrCreate()
    val sql = spark.sqlContext
    import sql.implicits._
    //week_3_ex_1
    //    val dates = Seq(
    //      "08/11/2015",
    //      "09/11/2015",
    //      "09/12/2015").toDF("date_string")
    //    val res = dates.withColumn("to_date", to_date(col("date_string"), "dd/MM/yyyy"))
    //                  .withColumn("diff", datediff(current_date(), col("to_date")))
    //    res.show(false)

    //week_3_ex_2
//    val words = Seq(Array("hello", "world")).toDF("words")
//    words.printSchema()
//    val solution = words.withColumn("solution", concat_ws(" ", col("words")))
//    solution.printSchema()
//    solution.show(false)

    //week_3_ex_3
    val salaries = spark
      .read
      .option("header", true)
      .option("inferSchema", true)
      .csv("src/resource/salaries.csv")
    salaries.show(false)

    val percentage = salaries.withColumn("rank", percent_rank() over Window.orderBy(col("Salary")))
                              .withColumn("Percentage", when(col("rank") > 0.7, "High")
                              .when(col("rank") > 0.3, "Average").otherwise("Low")).orderBy(col("rank").desc).drop(col("rank"))
    percentage.show(false)
  }
}
