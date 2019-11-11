import spark.implicits._
val checkin =  spark.sqlContext.read.json("/proj/cs784-PG0/checkin.json")
val business =  spark.sqlContext.read.json("/proj/cs784-PG0/business.json")

checkin.createOrReplaceTempView("checkin")
business.createOrReplaceTempView("business")

val join_output = spark.sql("Select * from business order by review_count, stars desc")
join_output.show(1000)

// Output business based on review count and rating in descending order
val join_output = spark.sql("Select * from business order by review_count, stars desc")
join_output.show(1000)


// Output best business based on review count >=10 and rating>=4.0 in descending order.
val best_business = spark.sql("select * from business where review_count > 10 and stars>= 4.0 order by
review_count,stars desc)

best_business.show()

// Output worst business based on review count >=10 and rating<=2.0 in descending order.
val worst_business = spark.sql("select * from business where review_count > 10 and stars<= 2.0 order by
review_count,stars desc)

worst_business.show()
