import spark.implicits._

val checkin =  spark.sqlContext.read.json("/mnt/data/checkin.json")
val business =  spark.sqlContext.read.json("/mnt/data/business.json")
business.createOrReplaceTempView("business")
checkin.createOrReplaceTempView("checkin")


val out_city_state_all = spark.sql("Select business.state as state, business.city as city from business JOIN checkin on
business.business_id = checkin.business_id")
out_city_state_all.coalesce(1).write.csv("/data/output/csv/out_city_state_all.csv")