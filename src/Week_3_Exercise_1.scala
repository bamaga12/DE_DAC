import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType

import java.text.SimpleDateFormat

object Week_3_Exercise_1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[1]").appName("SparkWeek3").getOrCreate()

    import spark.implicits._

    val dates = Seq(
      "08/11/2015",
      "09/11/2015",
      "09/12/2015").toDF("date_string")

    dates.show(false)
    val dateCoversion = dates.withColumn("to_date", to_date(unix_timestamp($"date_string", "dd/MM/yyyy").cast("Timestamp")))

    val dateDiff = dateCoversion.withColumn("diff", datediff(current_date(), $"to_date"))
    dateDiff.show(false)
  }
}
