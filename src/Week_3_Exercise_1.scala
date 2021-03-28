import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, current_date, datediff, first, max, min, to_date}

object Week_3_Exercise_1 {
//  spark: 2.4.7
//  scala: 2.12.10
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("thaolt34")
      .getOrCreate()

    import spark.sqlContext.implicits._

    val dates = Seq(
      "08/11/2015",
      "09/11/2015",
      "09/12/2015").toDF("date_string")

    val result = dates
      .withColumn("to_date", to_date(col("date_string"),"dd/MM/yyy"))
      .withColumn("diff", datediff(current_date(), col("to_date")))

    result.show
  }
}
