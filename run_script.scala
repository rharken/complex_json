//val df = spark.read.json("./applisttest.json");
//val df = spark.sql("SELECT id,explode(apps) as apps FROM parquet.`applist.parquet`");
//val df1 = df.withColumn("app", explode($"apps")).drop($"apps");
//val df2 = df1.withColumn("appName", $"app.appName").withColumn("appPackage", $"app.appPackage").withColumn("Ratings", explode($"app.Ratings")).drop($"app")
//df2.groupBy("id", "appName", "appPackage").avg("Ratings.Rating").orderBy("appName").show
//df2.groupBy("id", "appName", "appPackage").avg("Ratings.Rating").orderBy("appName").show
//df.write.mode("overwrite").parquet("applist.parquet")
//df.write.mode("overwrite").partitionBy("id").parquet("applist.parquet")
var df = spark.read.parquet("applist.parquet").select($"id",explode_outer($"apps"));
val df2 = df.withColumn("appName", $"col.appName").withColumn("appPackage", $"col.appPackage").withColumn("Ratings", explode($"col.Ratings")).drop($"col")
df2.groupBy("id", "appName", "appPackage").agg(avg($"Ratings.Rating").alias("avgRating")).orderBy("appName").show
System.exit(0)
