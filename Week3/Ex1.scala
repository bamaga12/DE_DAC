package Week3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Ex1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Ex1 week 3")
      .master("local")
      .getOrCreate()

    import spark.sqlContext.implicits._

    val dates = Seq(
      "08/11/2015",
      "09/11/2015",
      "09/12/2015").toDF("date_string")

    dates.show(false)

    //Spark sql 3.0.1
    //Scala 2.11.8
    val date2 = dates.withColumn("date", to_date(col("date_string"),"dd/MM/yyyy"))
    date2.show(false)

    val result = date2.select(col("date_string"), date_format(col("date"), "yyyy-MM-dd").as("to_date"), datediff(current_date(),col("date")).as("diff"))

    result.show(false)
  }
}
