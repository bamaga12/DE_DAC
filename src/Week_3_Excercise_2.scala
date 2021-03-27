import org.apache.spark.sql.SparkSession

object Week_3_Exercise_2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    val sql = spark.sqlContext

    import sql.implicits._
    val words = Seq(Array("hello", "world")).toDF("words")

    import org.apache.spark.sql.functions._
    val solution = words.withColumn("solution", concat_ws(" ", $"words"))

    solution.show()
  }
}