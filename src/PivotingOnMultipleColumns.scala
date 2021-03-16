// spark 2.4.7
// scala 2.12.13

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{first, max, min}


object PivotingOnMultipleColumns {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[1]").appName("SparkPractice").getOrCreate()
    import spark.implicits._

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

    //    data.show()

    val rsPrices = data.groupBy("id").pivot("day").agg(first("price")).orderBy("id")
    //    rsPrices.show()
    val new_prices=rsPrices.columns.map(c=>"price_"+ c)
    val prices = rsPrices.toDF(new_prices:_*)
    val tbl_prices = prices.withColumnRenamed("price_id", "id")
//    tbl_prices.show()

    val rsUnits = data.groupBy("id").pivot("day").agg(first("units"))
    //    rsUnits.show()
    val new_units=rsUnits.columns.map(c=>"unit_"+ c)
    val units = rsUnits.toDF(new_units:_*)
    val tbl_units = units.withColumnRenamed("unit_id", "id")
//    tbl_units.show()


    val result = tbl_prices.join(tbl_units, Seq("id")).orderBy("id")
    result.show()


  }

}
