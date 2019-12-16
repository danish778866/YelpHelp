import spark.implicits._

val review =  spark.sqlContext.read.json("/mnt/data/review.json")
val checkin =  spark.sqlContext.read.json("/mnt/data/checkin.json")
val business =  spark.sqlContext.read.json("/mnt/data/business.json")
review.createOrReplaceTempView("review")
business.createOrReplaceTempView("business")
checkin.createOrReplaceTempView("checkin")
Business_subset.createOrReplaceTempView("Business_subset")
Business_subset

val output_business_valet = spark.sql("Select attributes.BusinessParking from business where stars=5 and attributes.BusinessParking is not NULL")


val output_business_subset = spark.sql("Select count(*) from Business_subset")

val output_business_attributes_5 = spark.sql("Select COUNT(*) from business where stars=5 and attributes.RestaurantsTakeOut=True")

val output_business_attributes_4_5 = spark.sql("Select COUNT(*) from business where stars<5 and stars>=4.5 and attributes.RestaurantsTakeOut=True")

val output_business_attributes_4 = spark.sql("Select COUNT(*) from business where stars<4.5 and stars>=4 and attributes.RestaurantsTakeOut=True")

val output_business_attributes_3_5 = spark.sql("Select COUNT(*) from business where stars<4 and stars>=3.5 and attributes.RestaurantsTakeOut=True")

val output_business_attributes_3 = spark.sql("Select COUNT(*) from business where stars<3.5 and stars>=3 and attributes.RestaurantsTakeOut=True")

val output_business_attributes_2_5 = spark.sql("Select COUNT(*) from business where stars<3 and stars>=2.5 and attributes.RestaurantsTakeOut=True")

val output_business_attributes_2 = spark.sql("Select COUNT(*) from business where stars<2.5 and stars>=2 and attributes.RestaurantsTakeOut=True")

val output_business_attributes_1_5 = spark.sql("Select COUNT(*) from business where stars<2 and stars>=1.5 and attributes.RestaurantsTakeOut=True")

val output_business_attributes_1 = spark.sql("Select COUNT(*) from business where stars<1.5 and stars>=1 and attributes.RestaurantsTakeOut=True")

val output_business_attributes_0_5 = spark.sql("Select COUNT(*) from business where stars<1 and stars>=0.5 and attributes.RestaurantsTakeOut=True")

val output_business_attributes_0 = spark.sql("Select COUNT(*) from business where stars<0.5 and stars>=0 and attributes.RestaurantsTakeOut=True")



output_business_Valet_4_5.coalesce(1).write.csv("/data/output/csv/output_business_Valet_4_5.csv")
val output_business_Valet_5 = spark.sql("Select attributes from business where stars=5 and attributes.BusinessParking is not NULL")

val output_business_Valet_4_5 = spark.sql("Select attributes.BusinessParking from business where stars<5 and stars>=4.5 and attributes.BusinessParking is not NULL")

val output_business_Valet_4 = spark.sql("Select attributes.BusinessParking from business where stars<4.5 and stars>=4 and attributes.BusinessParking is not NULL")

val output_business_Valet_3_5 = spark.sql("Select attributes.BusinessParking from business where stars<4 and stars>=3.5 and attributes.BusinessParking is not NULL")

val output_business_Valet_3 = spark.sql("Select attributes.BusinessParking from business where stars<3.5 and stars>=3 and attributes.BusinessParking is not NULL")

val output_business_Valet_2_5 = spark.sql("Select attributes.BusinessParking from business where stars<3 and stars>=2.5 and attributes.BusinessParking is not NULL")

val output_business_Valet_2 = spark.sql("Select attributes.BusinessParking from business where stars<2.5 and stars>=2 and attributes.BusinessParking is not NULL")

val output_business_Valet_1_5 = spark.sql("Select attributes.BusinessParking from business where stars<2 and stars>=1.5 and attributes.BusinessParking is not NULL")

val output_business_Valet_1 = spark.sql("Select attributes.BusinessParking from business where stars<1.5 and stars>=1 and attributes.BusinessParking is not NULL")

val output_business_Valet_0_5 = spark.sql("Select attributes.BusinessParking from business where stars<1 and stars>=0.5 and attributes.BusinessParking is not NULL")

val output_business_Valet_0 = spark.sql("Select attributes.BusinessParking from business where stars<0.5 and stars>=0 and attributes.BusinessParking is not NULL")


val out_city_state = spark.sql("Select business.state as state, COUNT(*) as count from business JOIN checkin on business.business_id = checkin.business_id GROUP BY state order by count desc")
out_city_state.coalesce(1).write.csv("/data/output/csv/out_city_state.csv")


val out_city_state_all = spark.sql("Select business.state as state, business.city as city from business JOIN checkin on business.business_id = checkin.business_id")
out_city_state_all.coalesce(1).write.csv("/data/output/csv/out_city_state_all.csv")


val out_useful_5= spark.sql("Select business.business_id as businessId, SUM(review.useful) as totaluseful from business JOIN review on business.business_id = review.business_id where review.stars=5 group by businessId order by totaluseful desc")
out_useful_5.coalesce(1).write.csv("/data/output/csv/out_useful_5.csv")



val out_useful_4_5= spark.sql("Select business.business_id as businessId, SUM(review.useful) as totaluseful from business JOIN review on business.business_id = review.business_id where review.stars<5 and review.stars>=4.5 group by businessId order by totaluseful desc")
out_useful_4_5.coalesce(1).write.csv("/data/output/csv/out_useful_4_5.csv")

val out_useful_4= spark.sql("Select business.business_id as businessId, SUM(review.useful) as totaluseful from business JOIN review on business.business_id = review.business_id where review.stars<4.5 and review.stars>=4 group by businessId order by totaluseful desc")
out_useful_4.coalesce(1).write.csv("/data/output/csv/out_useful_4.csv")

val out_useful_3_5= spark.sql("Select business.business_id as businessId, SUM(review.useful) as totaluseful from business JOIN review on business.business_id = review.business_id where review.stars<4 and review.stars>=3.5 group by businessId order by totaluseful desc")
out_useful_3_5.coalesce(1).write.csv("/data/output/csv/out_useful_3_5.csv")

val out_useful_3= spark.sql("Select business.business_id as businessId, SUM(review.useful) as totaluseful from business JOIN review on business.business_id = review.business_id where review.stars<3.5 and review.stars>=3 group by businessId order by totaluseful desc")
out_useful_3.coalesce(1).write.csv("/data/output/csv/out_useful_3.csv")

val out_useful_2_5= spark.sql("Select business.business_id as businessId, SUM(review.useful) as totaluseful from business JOIN review on business.business_id = review.business_id where review.stars<3 and review.stars>=2.5 group by businessId order by totaluseful desc")
out_useful_2_5.coalesce(1).write.csv("/data/output/csv/out_useful_2_5.csv")

val out_useful_2= spark.sql("Select business.business_id as businessId, SUM(review.useful) as totaluseful from business JOIN review on business.business_id = review.business_id where review.stars<2.5 and review.stars>=2 group by businessId order by totaluseful desc")
out_useful_2.coalesce(1).write.csv("/data/output/csv/out_useful_2.csv")

val out_useful_1_5= spark.sql("Select business.business_id as businessId, SUM(review.useful) as totaluseful from business JOIN review on business.business_id = review.business_id where review.stars<2 and review.stars>=1.5 group by businessId order by totaluseful desc")
out_useful_1_5.coalesce(1).write.csv("/data/output/csv/out_useful_1_5.csv")

val out_useful_1= spark.sql("Select business.business_id as businessId, SUM(review.useful) as totaluseful from business JOIN review on business.business_id = review.business_id where review.stars<1.5 and review.stars>=1 group by businessId order by totaluseful desc")
out_useful_1.coalesce(1).write.csv("/data/output/csv/out_useful_1.csv")

val out_useful_0_5= spark.sql("Select business.business_id as businessId, SUM(review.useful) as totaluseful from business JOIN review on business.business_id = review.business_id where review.stars<1 and review.stars>=0.5 group by businessId order by totaluseful desc")
out_useful_0_5.coalesce(1).write.csv("/data/output/csv/out_useful_0_5.csv")

val out_useful_0= spark.sql("Select business.business_id as businessId, SUM(review.useful) as totaluseful from business JOIN review on business.business_id = review.business_id where review.stars<0.5 and review.stars>=0 group by businessId order by totaluseful desc")
out_useful_0.coalesce(1).write.csv("/data/output/csv/out_useful_0.csv")