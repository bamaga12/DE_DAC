import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat_ws, current_date, datediff, to_date}

object Week_3_Exercise_2 {
  //  spark: 2.4.7
  //  scala: 2.12.10
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("thaolt34")
      .getOrCreate()

    import spark.sqlContext.implicits._

    val words = Seq(Array("hello", "world")).toDF("words")

    val result = words.withColumn("solution", concat_ws(" ", col("words")))

    result.show
  }
}
