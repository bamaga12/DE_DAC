import org.apache.spark.sql.SparkSession

object Week_3_Exercise_1 {
  //Spark 3.1.1 - SCALA 2.12

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Simple Application").config("spark.master", "local[*]").getOrCreate()
    import spark.implicits._

    val dates = Seq(
      "08/11/2015",
      "09/11/2015",
      "09/12/2015").toDF("date_string")

    dates.createOrReplaceTempView("date")
    val result1 = spark.sql("SELECT date_string,CURRENT_DATE as to_date, datediff(to_date(CURRENT_DATE),to_date(from_unixtime(unix_timestamp(date_string,'dd/MM/yyy'),'yyyy-MM-dd'))) as diff FROM date")
    result1.show(false)
  }
}