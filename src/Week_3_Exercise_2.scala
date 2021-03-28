import org.apache.spark.sql.SparkSession

object Week_3_Exercise_2 {
  //Spark 3.1.1 - SCALA 2.12

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Simple Application").config("spark.master", "local[*]").getOrCreate()
    import spark.implicits._

    val words = Seq(Array("hello", "world")).toDF("words")
    words.createOrReplaceTempView("wordsdf")
    val result1 = spark.sql("SELECT words, concat_ws(' ', words) AS solution FROM wordsdf")
    result1.show(false)
  }
}
