// bt 1
val nums = spark.range(5).withColumn("group", 'id % 2)
nums.groupBy("group").agg(max("id").as("max_id"),min("id").as("min_id")).show()

// bt 2
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

val df = data.groupBy("id").pivot("day").agg(first("price").alias("price"),first("units").alias("unit"))
