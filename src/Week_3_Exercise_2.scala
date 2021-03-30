import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Week_3_Exercise_2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[1]").appName("SparkWeek3").getOrCreate()

    import spark.implicits._

    val words = Seq(Array("hello", "world")).toDF("words")
    words.show(false)
    words.printSchema()

    val solution = words.withColumn("solution", concat_ws(" ", $"words"))
    solution.show(false)
    solution.printSchema()
  }
}
