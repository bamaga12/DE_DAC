import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{asc, col, current_date, datediff, to_date}

object Week_3_Exercise_1 {
  def main(args: Array[String]): Unit = {
    // create spark session
    val spark = SparkSession
      .builder()
      .appName("anhvt105")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val dates = Seq(
      "08/11/2015",
      "09/11/2015",
      "09/12/2015").toDF("date_string")

    val result = dates
      .withColumn("to_date",to_date(col("date_string"),"dd/MM/yyyy"))
      .withColumn("diff",datediff(current_date(),col("to_date")))
      .orderBy(asc("date_string"))

    result.show()
  }
}
