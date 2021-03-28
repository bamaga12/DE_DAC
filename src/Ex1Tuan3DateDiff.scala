import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions._

object Ex1Tuan3DateDiff {
  val spark = SparkSession.builder().master("local[1]").appName("SparkPractice").getOrCreate();
  import spark.implicits._
  val sqlContext: SQLContext = spark.sqlContext

  def main(args: Array[String]): Unit = {

    val dates = Seq(
      "08/11/2015",
      "09/11/2015",
      "09/12/2015").toDF("date_string");

    dates.show();

    val dateStringToDate = dates.withColumn("to_date", to_date(col("date_string"),"dd/MM/yyyy"))
                                .withColumn("diff", datediff(current_date(),col("to_date")));
    dateStringToDate.show();

  }
}
