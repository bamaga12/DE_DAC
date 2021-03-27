import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{asc, col, concat_ws}

object Week_3_Exercise_2 {
  def main(args: Array[String]): Unit = {
    // create spark session
    val spark = SparkSession
      .builder()
      .appName("anhvt105")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val words = Seq(Array("hello", "world")).toDF("words")

    val result = words
      .withColumn("solution",concat_ws(" ",col("words")))
    result.show()
  }
}
