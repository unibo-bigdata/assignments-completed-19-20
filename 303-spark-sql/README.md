# 303 SparkSQL

Module 1, Big Data course (81932), University of Bologna.

## 303-0 Load new datasets

If you are working on the Virtual Machine, load the new datasets in the 
```/bigdata/dataset``` folder on HDFS (follow instructions from 101).

Note: file ```dataset/postcodes/geo.parquet``` must be loaded also as a Hive Table. From Spark's shell:

```shell
# Spark 1
val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
hiveContext.sql("use default")
hiveContext.read.load("/bigdata/dataset/postcodes/geo.parquet").write.saveAsTable("parquet_table")
# Spark 2
spark.sql("use default")
spark.read.format("parquet").load("/bigdata/dataset/postcodes/geo.parquet").write.saveAsTable("parquet_table")
```

## 303-1 Create DataFrames

Load a JSON file (Movies)

```shell
# Spark 1
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val dfMovies = sqlContext.jsonFile("/bigdata/dataset/movies/movies.json")
# Spark 2
val dfMovies = spark.read.json("/bigdata/dataset/movies/movies.json")
val dfMovies = spark.read.format("json").load("/bigdata/dataset/movies/movies.json")
```

Load a CSV without a defined schema (Population)

```shell
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType,StructField,StringType}
val population = sc.textFile("/bigdata/dataset/population/zipcode_population.csv")
val schemaString = "zipcode total_population avg_age male female"
val schema = StructType(schemaString.split(" ").map(fieldName ⇒ StructField(fieldName, StringType, true)))
val rowRDD = population.map(_.split(";")).map(e ⇒ Row(e(0), e(1), e(2), e(3), e(4)))
val dfPeople = spark.createDataFrame(rowRDD, schema)
```

Load a TXT with the schema in the first row (Real estate transactions)

```shell
# Spark 1
val transaction_RDD = sc.textFile("/bigdata/dataset/real_estate/real_estate_transactions.txt")
val schema_array = transaction_RDD.take(1)
val schema_string = schema_array(0)
val schema = StructType(schema_string.split(';').map(fieldName ⇒ StructField(fieldName, StringType, true)))
val rowRDD = transaction_RDD.map(_.split(";")).map(e ⇒ Row(e(0), e(1), e(2), e(3), e(4)))
val dfTransactionsTmp = sqlContext.createDataFrame(rowRDD, schema)
val dfTransactions = dfTransactionsTmp.where("street <> 'street'")
# Spark 2
val dfTransactions = spark.read.format("csv").option("header", "true").option("delimiter",";").load("/bigdata/dataset/real_estate/real_estate_transactions.txt")
```

Load a Parquet file (User data)

```shell
# Spark 1
val dfUserData = sqlContext.read.load("/bigdata/dataset/userdata/userdata.parquet")
# Spark 2
val dfUserData = spark.read.format("parquet").load("/bigdata/dataset/userdata/userdata.parquet")
```

Load a Hive table (Geography)

```shell
# Spark 1
val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
hiveContext.sql("use default")
val dfGeo = hiveContext.sql("select * from geo")
# Spark 2
spark.sql("use default")
val dfGeo = spark.sql("select * from geo")
```

## 303-2 Write DataFrames

Write to JSON on HDFS

```shell
peopleDF.write.mode("append").json("people.json")
```

Write to table on Hive (create a new database, select it, and write the table). 
First, create a database with name "user_[username]" (e.g., "user_egallinucci").

```shell
# Spark 1: Need to reload parquet_df with the hiveContext object
hiveContext.sql("create database user_[username]")
hiveContext.sql("use user_[username]")
val parquet_df = hiveContext.read.load("/bigdata/dataset/userdata/userdata.parquet")
# Spark 2
spark.sql("create database user_[username]")
spark.sql("use user_[username]")
# Spark 1 and 2
parquet_df.write.saveAsTable("parquet_table")
parquet_df.write.mode("overwrite").saveAsTable("parquet_table")
parquet_df.write.format("parquet").mode("overwrite").saveAsTable("parquet_table")
```

Write to Parquet file on HDFS

```shell
df_movies.write.parquet("movies.parquet")
```

## 303-3 Basic SQL Operations

```shell
val dfWeather = sc.textFile("hdfs:/bigdata/dataset/weather-sample").map(WeatherData.extract).toDF()
val dfStation = sc.textFile("hdfs:/bigdata/dataset/weather-info/stations.csv").map(StationData.extract).toDF()

# Spark 1
dfWeather.registerTempTable("weather")
dfStation.registerTempTable("station")
val dfJoin = sqlContext.sql("select w.*, s.* from weather w, station s where w.usaf=s.usaf and w.wban = s.wban")
# Spark 2
dfWeather.createOrReplaceTempView("weather")
dfStation.createOrReplaceTempView("station")
val dfJoin = spark.sql("select w.*, s.* from weather w, station s where w.usaf=s.usaf and w.wban = s.wban")

dfJoin.printSchema()

val q1 = dfJoin.filter("elevation > 100").select("country","elevation")
val q2 = dfJoin.groupBy("country").agg(avg("temperature")).orderBy(asc("country"))
val q3 = dfJoin.groupBy("country").agg(avg("temperature").as("avgTemp")).orderBy(desc("avgTemp"))
q1.show()
q2.show()
q3.show()
```

Try out some queries on the available data frames. For instance:
- Given dfTransactions, calculate the average price per city and show the most expensive cities first.
- Also, convert from EUR to USD (considering an exchange rate of 1€ = 1.2$); i.e., multiply the price by 1.2. 
- Save the result to a Parquet file.

```shell
# Spark 1
dfTransactions.registerTempTable("transactions")
val q5 = sqlContext.sql("select city, round(avg(price*1.2),2) as avgPrice from transactions group by city order by avgPrice desc")
q5.show()
q5.write.parquet("transactionPrices.parquet")

# Spark 2
dfTransactions.createOrReplaceTempView("transactions")
val q5 = spark.sql("select city, round(avg(price*1.2),2) as avgPrice from transactions group by city order by avgPrice desc")
q5.show()
q5.write.parquet("transactionPrices.parquet")
```

## 303-4 Execution plan evaluation

Verify predicate push-down: in both cases, selection predicates are pushed as close to the source as possible.

```
# Spark 1
val q4 = sqlContext.sql("select w.*, s.* from weather w, station s where w.usaf=s.usaf and w.wban = s.wban and elevation > 100")
# Spark 2
val q4 = spark.sql("select w.*, s.* from weather w, station s where w.usaf=s.usaf and w.wban = s.wban and elevation > 100")

q1.explain
q4.explain
```

Evaluate broadcasting: check execution times and the DAGs in the Web UI.

```
val dfJoin2 = dfWeather.join(broadcast(dfStation),dfWeather("usaf")===dfStation("usaf") && dfWeather("wban") === dfStation("wban"))
# Latest Spark's versions (after 2.1)
val dfJoin2 = spark.sql("/*+ broadcast(s) */ select w.*, s.* from weather w, station s where w.usaf=s.usaf and w.wban = s.wban")

dfJoin2.explain
dfJoin.explain

dfJoin2.count()
dfJoin.count()
```
