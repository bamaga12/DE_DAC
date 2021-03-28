import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions._

object Ex2Tuan3ArraysToString {

  val spark = SparkSession.builder().master("local[1]").appName("SparkPractice").getOrCreate();
  import spark.implicits._
  val sqlContext: SQLContext = spark.sqlContext

  def main(args: Array[String]): Unit = {

    val words = Seq(Array("hello", "world")).toDF("words");
    val results = words.as[(Array[String])]
                        .map(x => (x,x.mkString(" ")))
                        .toDF("words","solution")
    results.show();

  }

}
