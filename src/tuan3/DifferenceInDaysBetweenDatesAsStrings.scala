// spark 2.4.7
// scala 2.12.13

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, current_date, datediff, to_date, unix_timestamp}

object DifferenceBetweenDatesAsStrings {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[1]").appName("SparkPractice").getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    val dates = Seq(
      "08/11/2015",
      "09/11/2015",
      "09/12/2015").toDF("date_string")
    //    dates.show

    //    dates.withColumn("date_string", to_date($"date_string")).show
    //    val result = dates.withColumn("date_string", to_date($"date_string", "dd/MM/yyyy").alias("to_date"))
    //    val result = dates.withColumn("date_string", datediff(current_date(),to_date($"date_string", "dd/MM/yyyy")).as("datediff"))

    val result = dates.select(col("date_string"), to_date($"date_string", "dd/MM/yyyy").alias("to_date"), datediff(current_date(),to_date($"date_string", "dd/MM/yyyy")).as("datediff"))
    result.show()

  }
}