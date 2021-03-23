package com.sonkc.tuan3.bai1

import org.apache.spark.sql.SparkSession

object bai1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkByExamples")
      .master("local[1]")
      .getOrCreate()
    val sql = spark.sqlContext
    import sql.implicits._
    import org.apache.spark.sql.functions._
    val dates = Seq(
      "08/11/2015",
      "09/11/2015",
      "09/12/2015").toDF("date_string")

    val result = dates.select(col("date_string"),
      to_date(col("date_string"),"dd/MM/yyyy").as("to_date"))
    result.select(col("date_string"),
                col("to_date"),datediff(current_date(),
                col("to_date")).as("diff"))
                .show(false)
    /*dates.select(col("date_string"),
                 to_date(col("date_string"),"dd/MM/yyyy").as("to_date"),
                  datediff(current_date(),col("to_date")).as("diff"))
                  .show(false)*/
  }
}
