import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window



object main {

  //config Spark
  val sparkConf = new SparkConf()
  sparkConf.setAppName("Demo")
  sparkConf.setMaster("local")
  val sc = new SparkContext(sparkConf)
  val spark = SparkSession.builder().appName("Demo").master("local").getOrCreate()

  import  spark.implicits._

  def main(args: Array[String]): Unit = {

    //Exercise1
    //    val dates = Seq(
    //      "08/11/2015",
    //      "09/11/2015",
    //      "09/12/2015").toDF("date_string")
    //
    //    dates.select(col("date_string")
    //      , to_date(col("date_string"), "dd/MM/yyyy").as("to_date")
    //      , datediff(current_date(), to_date(col("date_string"), "dd/MM/yyyy").as("to_date")).as("diff")
    //    )
    //      .show()

    //Exercise2
    //    val words = Seq(Array("hello", "world")).toDF("words")
    //
    //    val solution = words.select(col("words")
    //      , concat_ws(" ", col("words").getItem(0), col("words").getItem(1)).as("solution"))
    //    solution.printSchema()
    //    solution.show()
  }

  //Exercise3
  val salaries = spark
    .read
    .option("header", true)
    .option("inferSchema", true)
    .csv("C:\\Users\\ASUS\\IdeaProjects\\DE_DAC\\MEOMEO\\resource\\salaries.csv")

  val temp = Window.orderBy("Salary")
  val resultEx3 = salaries.withColumn("percent_rank", percent_rank over(temp)).selectExpr("Employee"
    , "Salary", "(case when round(percent_rank,1) >= 0.7 then 'High'" +
      " when round(percent_rank,1) >= 0.6 and round(percent_rank,1) < 0.7 then 'Average'" +
      " else 'Low' end) as Percentage").orderBy('Salary.desc)
  resultEx3.show()
}