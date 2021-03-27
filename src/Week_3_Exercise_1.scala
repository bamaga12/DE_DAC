import org.apache.spark.sql.SparkSession

object Week_3_Exercise_1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("hieund28").master("local").getOrCreate()
    val sql = spark.sqlContext

    import sql.implicits._
    val dates = Seq(
      "08/11/2015",
      "09/11/2015",
      "09/12/2015").toDF("date_string")

    import org.apache.spark.sql.functions._
    val rs1 = dates.select($"*",
      to_date($"date_string", "dd/MM/yyyy").as("to_date")).withColumn("cur_date", current_date())

    val rs2 = rs1.select($"date_string", $"to_date", datediff($"cur_date", $"to_date").as("diff"))
    rs2.show()

  }
}