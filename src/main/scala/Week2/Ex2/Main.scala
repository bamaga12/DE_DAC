package Week2.Ex2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Exercise_1")
      .master("local")
      .getOrCreate()

    import spark.sqlContext.implicits._

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

    data.show(false)

    val tmp_units = data.groupBy($"id").pivot("day").agg(first("units"))
    val units_value = tmp_units.toDF("id","unit_1","unit_2","unit_3","unit_4")
    val tmp_price = data.groupBy($"id").pivot("day").agg(first("price"))
    val price_value = tmp_price.toDF("id","price_1","price_2","price_3","price_4")
    val result = price_value.join(units_value,Seq("id"),"inner").orderBy("id")

    result.show(false)
  }
}
