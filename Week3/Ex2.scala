package Week3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Ex2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Ex2 week 3")
      .master("yarn")
      .getOrCreate()

    import spark.sqlContext.implicits._

    val words = Seq(Array("hello", "world")).toDF("words")
    words.show(false)

    val solution = words.withColumn("solution", concat_ws(" ",col("words")))

    solution.show(false)
  }
}
