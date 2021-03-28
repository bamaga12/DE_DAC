import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions._

object Ex3Tuan3CalcPercentRank {

  val spark = SparkSession.builder().master("local[1]").appName("SparkPractice").getOrCreate();
  import spark.implicits._
  val sqlContext: SQLContext = spark.sqlContext

  def main(args: Array[String]): Unit = {

    val salaries = spark.read.json("src/resources/salaries.json");

    val result = salaries.withColumn("Percentage", percent_rank over Window.orderBy($"Salary".desc))
            .withColumn("Percentage", when($"Percentage" < 0.4, "High")
                                              .when($"Percentage" > 0.5, "Low")
                                              .otherwise("Average"));
    result.show();

  }

}
