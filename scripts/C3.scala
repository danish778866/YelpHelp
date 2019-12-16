import spark.implicits._

val review =  spark.sqlContext.read.json("/mnt/data/review.json")
val business =  spark.sqlContext.read.json("/mnt/data/business.json")
review.createOrReplaceTempView("review")
business.createOrReplaceTempView("business")


val out_useful_5= spark.sql("Select business.business_id as businessId, SUM(review.useful) as totaluseful from business
JOIN review on business.business_id = review.business_id where review.stars=5 group by businessId order by totaluseful
desc")
out_useful_5.coalesce(1).write.csv("/data/output/csv/out_useful_5.csv")

val out_useful_4_5= spark.sql("Select business.business_id as businessId, SUM(review.useful) as totaluseful from
business JOIN review on business.business_id = review.business_id where review.stars<5 and review.stars>=4.5 group
by businessId order by totaluseful desc")
out_useful_4_5.coalesce(1).write.csv("/data/output/csv/out_useful_4_5.csv")

val out_useful_4= spark.sql("Select business.business_id as businessId, SUM(review.useful) as totaluseful from business
JOIN review on business.business_id = review.business_id where review.stars<4.5 and review.stars>=4 group by businessId
order by totaluseful desc")
out_useful_4.coalesce(1).write.csv("/data/output/csv/out_useful_4.csv")

val out_useful_3_5= spark.sql("Select business.business_id as businessId, SUM(review.useful) as totaluseful from
business JOIN review on business.business_id = review.business_id where review.stars<4 and review.stars>=3.5 group by
businessId order by totaluseful desc")
out_useful_3_5.coalesce(1).write.csv("/data/output/csv/out_useful_3_5.csv")

val out_useful_3= spark.sql("Select business.business_id as businessId, SUM(review.useful) as totaluseful from business
JOIN review on business.business_id = review.business_id where review.stars<3.5 and review.stars>=3 group by businessId
order by totaluseful desc")
out_useful_3.coalesce(1).write.csv("/data/output/csv/out_useful_3.csv")

val out_useful_2_5= spark.sql("Select business.business_id as businessId, SUM(review.useful) as totaluseful from
business JOIN review on business.business_id = review.business_id where review.stars<3 and review.stars>=2.5 group by
businessId order by totaluseful desc")
out_useful_2_5.coalesce(1).write.csv("/data/output/csv/out_useful_2_5.csv")

val out_useful_2= spark.sql("Select business.business_id as businessId, SUM(review.useful) as totaluseful from business
JOIN review on business.business_id = review.business_id where review.stars<2.5 and review.stars>=2 group by businessId
order by totaluseful desc")
out_useful_2.coalesce(1).write.csv("/data/output/csv/out_useful_2.csv")

val out_useful_1_5= spark.sql("Select business.business_id as businessId, SUM(review.useful) as totaluseful from
business JOIN review on business.business_id = review.business_id where review.stars<2 and review.stars>=1.5 group by
businessId order by totaluseful desc")
out_useful_1_5.coalesce(1).write.csv("/data/output/csv/out_useful_1_5.csv")

val out_useful_1= spark.sql("Select business.business_id as businessId, SUM(review.useful) as totaluseful from business
JOIN review on business.business_id = review.business_id where review.stars<1.5 and review.stars>=1 group by businessId
order by totaluseful desc")
out_useful_1.coalesce(1).write.csv("/data/output/csv/out_useful_1.csv")

val out_useful_0_5= spark.sql("Select business.business_id as businessId, SUM(review.useful) as totaluseful from
business JOIN review on business.business_id = review.business_id where review.stars<1 and review.stars>=0.5 group by
businessId order by totaluseful desc")
out_useful_0_5.coalesce(1).write.csv("/data/output/csv/out_useful_0_5.csv")

val out_useful_0= spark.sql("Select business.business_id as businessId, SUM(review.useful) as totaluseful from business
JOIN review on business.business_id = review.business_id where review.stars<0.5 and review.stars>=0 group by businessId
order by totaluseful desc")
out_useful_0.coalesce(1).write.csv("/data/output/csv/out_useful_0.csv")