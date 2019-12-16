import spark.implicits._

val review =  spark.sqlContext.read.json("/mnt/data/review.json")
val checkin =  spark.sqlContext.read.json("/mnt/data/checkin.json")
val business =  spark.sqlContext.read.json("/mnt/data/business.json")
review.createOrReplaceTempView("review")
business.createOrReplaceTempView("business")
checkin.createOrReplaceTempView("checkin")

#Best business:
spark.sql("Select COUNT(*) from business where attributes.RestaurantsTakeOut=True andstars >= 4.0")

#Worst business:
spark.sql ("select * from business where review_count > 10 and stars <=  2.5 order by review_count,stars desc")