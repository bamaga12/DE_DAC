import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DataType, DataTypes}

object Main {
  val spark = SparkSession.builder().master("local[1]").appName("SparkPractice").getOrCreate();
  import spark.implicits._
  val sqlContext: SQLContext = spark.sqlContext

  def main(args: Array[String]): Unit = {


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

    val data1 = data.withColumn("price_1", lit("price_"))
      .withColumn("price_2", concat($"price_1",$"day"))
      .withColumn("unit_1", lit("unit_"))
      .withColumn("unit_2", concat($"unit_1",$"day"))
    val pivotPrice = data1.groupBy("id").pivot("price_2").agg(first("price").as("price"))
    val pivotUnits = data1.groupBy("id").pivot("unit_2").agg(first("units"))
    val result = pivotPrice.join(pivotUnits, Seq("id")).orderBy("id")
    result.show()
  }
}
