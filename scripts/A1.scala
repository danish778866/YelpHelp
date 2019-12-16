import spark.implicits._

val business =  spark.sqlContext.read.json("/mnt/data/business.json")
business.createOrReplaceTempView("business")


spark.sql("Select COUNT(*) from business where city="San Francisco" and stars>=4.0")