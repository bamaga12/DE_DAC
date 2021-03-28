import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object week3_exercise1{
def main(args: Array[String]): Unit = {
  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .getOrCreate()
import spark.implicits._

  val dates = Seq(
    "08/11/2015",
    "09/11/2015",
    "09/12/2015").toDF("date_string")

  dates.withColumn("to_date",to_date(col("date_string"),"dd/MM/yyyy"))
    .withColumn("diff",datediff(current_date(),to_date(col("date_string"),"dd/MM/yyyy")))
    .show(false)

  }
}
