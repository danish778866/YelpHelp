import spark.implicits._

val business =  spark.sqlContext.read.json("/mnt/data/business.json")
business.createOrReplaceTempView("business")

val output_business_Valet_5 = spark.sql("Select attributes from business where stars=5 and
attributes.BusinessParking is not NULL")
output_business_Valet_5.coalesce(1).write.csv("/data/output/csv/output_business_Valet_5.csv")

val output_business_Valet_4_5 = spark.sql("Select attributes.BusinessParking from business where stars<5 and
stars>=4.5 and attributes.BusinessParking is not NULL")
output_business_Valet_4_5.coalesce(1).write.csv("/data/output/csv/output_business_Valet_4_5.csv")

val output_business_Valet_4 = spark.sql("Select attributes.BusinessParking from business where stars<4.5 and
stars>=4 and attributes.BusinessParking is not NULL")
output_business_Valet_4.coalesce(1).write.csv("/data/output/csv/output_business_Valet_4.csv")

val output_business_Valet_3_5 = spark.sql("Select attributes.BusinessParking from business where stars<4 and
stars>=3.5 and attributes.BusinessParking is not NULL")
output_business_Valet_3_5.coalesce(1).write.csv("/data/output/csv/output_business_Valet_3_5.csv")

val output_business_Valet_3 = spark.sql("Select attributes.BusinessParking from business where stars<3.5 and
stars>=3 and attributes.BusinessParking is not NULL")
output_business_Valet_3.coalesce(1).write.csv("/data/output/csv/output_business_Valet_3.csv")

val output_business_Valet_2_5 = spark.sql("Select attributes.BusinessParking from business where stars<3 and
stars>=2.5 and attributes.BusinessParking is not NULL")
output_business_Valet_2_5.coalesce(1).write.csv("/data/output/csv/output_business_Valet_2_5.csv")

val output_business_Valet_2 = spark.sql("Select attributes.BusinessParking from business where stars<2.5 and
stars>=2 and attributes.BusinessParking is not NULL")
output_business_Valet_2.coalesce(1).write.csv("/data/output/csv/output_business_Valet_2.csv")

val output_business_Valet_1_5 = spark.sql("Select attributes.BusinessParking from business where stars<2 and
stars>=1.5 and attributes.BusinessParking is not NULL")
output_business_Valet_1_5.coalesce(1).write.csv("/data/output/csv/output_business_Valet_1_5.csv")

val output_business_Valet_1 = spark.sql("Select attributes.BusinessParking from business where stars<1.5 and
stars>=1 and attributes.BusinessParking is not NULL")
output_business_Valet_1.coalesce(1).write.csv("/data/output/csv/output_business_Valet_1.csv")

val output_business_Valet_0_5 = spark.sql("Select attributes.BusinessParking from business where stars<1 and
stars>=0.5 and attributes.BusinessParking is not NULL")
output_business_Valet_0_5.coalesce(1).write.csv("/data/output/csv/output_business_Valet_0_5.csv")

val output_business_Valet_0 = spark.sql("Select attributes.BusinessParking from business where stars<0.5 and
stars>=0 and attributes.BusinessParking is not NULL")
output_business_Valet_0.coalesce(1).write.csv("/data/output/csv/output_business_Valet_0.csv")