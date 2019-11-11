import spark.implicits._
val checkin =  spark.sqlContext.read.json("/proj/cs784-PG0/checkin.json")
val business =  spark.sqlContext.read.json("/proj/cs784-PG0/business.json")

checkin.createOrReplaceTempView("checkin")
business.createOrReplaceTempView("business")

val join_output = spark.sql("Select * from business order by review_count, stars desc")
join_output.show(1000)
