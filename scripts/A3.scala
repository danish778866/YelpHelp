import spark.implicits._

val business =  spark.sqlContext.read.json("/mnt/data/business.json")
business.createOrReplaceTempView("business")

spark.sql ("select * from  business where review_count > 10 and stars <= 2.0 order by review_count , stars desc" )