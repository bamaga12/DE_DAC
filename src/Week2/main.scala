import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

object main {

  //config Spark
  val sparkConf = new SparkConf()
  sparkConf.setAppName("Demo")
  sparkConf.setMaster("local")
  val sc = new SparkContext(sparkConf)
  val spark = SparkSession.builder().appName("Demo").master("local").getOrCreate()

  import spark.implicits._

  def main(args: Array[String]): Unit = {

    //    //Exercise1
    //    val num = spark.range(5).select($"id", ($"id" % 2).as("group"))
    //    val resultEx1 = num.groupBy('group).agg(max("id").as("max_id"), min("id").as(("min_id")))
    //    resultEx1.show()

    //Exercise2
    val data = Seq(
      (100,1,23,10),
      (100,2,45,11),
      (100,3,67,12),
      (100,4,78,13),
      (101,1,23,10),
      (101,2,45,13),
      (101,3,67,14),
      (101,4,78,15),
      (102,1,23,10),
      (102,2,45,11),
      (102,3,67,16),
      (102,4,78,18)).toDF("id", "day", "price", "units")

    val pivotPrice = data.groupBy("id").pivot("day").max("price")
      .select(col("id").as("idPrice"), col("1").as("price_1"), col("2").as("price_2")
        , col("3").as("price_3")
        , col("4").as("price_4")
      )
    val pivotUnit = data.groupBy("id").pivot("day").max("units")
      .select(col("id").as("idUnit"), col("1").as("unit_1"), col("2").as("unit_2")
        , col("3").as("unit_3")
        , col("4").as("unit_4")
      )
    val resultEx2 = pivotPrice.join(pivotUnit, col("idPrice") === col("idUnit"), "inner").orderBy(col("idPrice").asc)
    resultEx2.show()
  }

}