// bt 1
val dates = Seq(
   "08/11/2015",
   "09/11/2015",
   "09/12/2015").toDF("date_string")
dates.withColumn("to_date",to_date(col("date_string"),"dd/MM/yyyy")).withColumn("diff",datediff(current_date(),col("to_date"))).show()

// bt 2
val words = Seq(Array("hello", "world")).toDF("words")
words.withColumn("solution",concat_ws(" ",col("words"))).show()

// bt 3
val df = spark.read.format("csv").option("header", "true").load("C:/Users/chieu/Downloads/de.csv")
val df2 = df.withColumn("Percentage", percent_rank over Window.orderBy($"Salary".desc)).withColumn("Percentage", when($"Percentage" >= 0.7, "Low").when($"Percentage" <= 0.3, "High").otherwise("Average"))
