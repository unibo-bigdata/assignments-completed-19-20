# 302 Spark optimization

Module 1, Big Data course (81932), University of Bologna.

File ```src/main/scala/Exercise``` contains the code for this assignment. Complete the code and/or answer the provided questions.

To run via spark shell:
- ```spark-shell --num-executors 1``` (on the VM)
- ```spark2-shell``` (on the cluster)
- Copy/paste the code in ```src/main/scala/StationData``` and ```src/main/scala/WeatherData``` (use ```:paste``` to enter paste mode; use Ctrl+D to exit it)
- Copy/paste and complete exercise code (note: there is no need to create the SparkContext)

To tun via spark submit:
- Comment/uncomment dependencies and code lines code lines (see function ```getSparkContext()```) depending on your Spark version
- ```spark-submit --class Exercise BD-302-spark-opt.jar <exerciseNumber>```
- ```spark2-submit --class Exercise BD-302-spark-opt.jar <exerciseNumber>```
- Beware: debugging is more burdensome (use ```yarn logs -applicationId <appId>``` where ```<appId>``` is similar to ```application_1583679666662_0134``` to retrieve the Executors' logs)

## 302-1 Job optimization

Optimize the two jobs (avg temperature and max temperature) by avoiding the repetition of the same computations and by defining a good number of partitions.

Hints:
- Verify your persisted data in the web UI
- Use either ```repartition()``` or ```coalesce()``` to define the number of partitions
  - ```repartition()``` shuffles all the data
  - ```coalesce()``` minimizes data shuffling by exploiting the existing partitioning
- Verify the execution plan of your RDDs with ```rdd.toDebugString``` (shell) or on the web UI

Solution is in the code.

## 302-2 RDD preparation

Check the four possibilities to transform the Station RDD and understand which is the best one.

Solution: #3.

## 302-3 Joining RDDs

Define the join between rddWeather and rddStation and compute:
- The maximum temperature for every city
- The maximum temperature for every city in Italy: 
  - ```StationData.country == "IT"```
- Sort the results by descending temperature
  - ```map({case(k,v)=>(v,k)})``` to invert key with value and vice versa

Hints & considerations:
- Keep only temperature values <999
- Join syntax: ```rdd1.join(rdd2)```
- Both RDDs should be structured as key-value RDDs with the same key: usaf + wban
- Consider partitioning and caching to optimize the join
- Careful: it is not enough for the two RDDs to have the same number of partitions; they must have the same partitioner!
- Verify the execution plan of the join in the web UI

Solution is in the code.

## 302-4 Memory occupation

Use Spark's web UI to verify the space occupied by the provided RDDs.

## 302-5 Evaluating different join methods

Consider the following scenario:
- We have a disposable RDD of Weather data (i.e., it is used only once): ```rddW```
- And we have an RDD of Station data that is used many times: ```rddS```
- Both RDDs are cached (```collect() ```is called to enforce caching)

We want to join the two RDDS. Which option is best?
- Simply join the two RDDs
- Enforce on ```rddW1``` the same partitioner of ```rddS``` (and then join)
- Exploit broadcast variables

Solution:
- Simple join: directly shuffles rddW to rddS's partitions
- Partition forcing: slightly more expensive, as rddW partitions still need to be shuffled after applying partitioning criteria
- Broadcast: is the most efficient, but it works only if the smaller dataset is very small