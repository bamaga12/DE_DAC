import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object week3_exercise2{
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._

    val words = Seq(Array("hello", "world")).toDF("words")

    words.printSchema()

    val solution = words.withColumn("solution",concat_ws(",",col("words")))

    solution.show(false)
    solution.printSchema()
  }
}