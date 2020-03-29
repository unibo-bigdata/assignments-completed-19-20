import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession


// spark2-submit --class Exercise BD-302-spark-opt.jar <exerciseNumber>
// spark-submit --class Exercise BD-302-spark-opt.jar <exerciseNumber>
object Exercise extends App {

  override def main(args: Array[String]): Unit = {
    val sc = getSparkContext()

    if(args.length >= 1){
      args(0) match {
        case "1" => exercise1(sc)
        case "2" => exercise2(sc)
        case "3" => exercise3(sc)
        case "4" => exercise4(sc)
        case "5" => exercise5(sc)
      }
    }
  }

  /**
   * Creates the SparkContent; comment/uncomment code depending on Spark's version!
   * @return
   */
  def getSparkContext(): SparkContext = {
    // Spark 1
    // val conf = new SparkConf().setAppName("Exercise 302 - Spark1")
    // new SparkContext(conf)

    // Spark 2
    val spark = SparkSession.builder.appName("Exercise 302 - Spark2").getOrCreate()
    spark.sparkContext
  }

  /**
   * Optimize the two jobs (avg temperature and max temperature)
   * by avoiding the repetition of the same computations
   * and by defining a good number of partitions.
   *
   * Hints:
   * - Verify your persisted data in the web UI
   * - Use either repartition() or coalesce() to define the number of partitions
   *   - repartition() shuffles all the data
   *   - coalesce() minimizes data shuffling by exploiting the existing partitioning
   * - Verify the execution plan of your RDDs with rdd.toDebugString (shell) or on the web UI
   *
   * @param sc
   */
  def exercise1(sc: SparkContext): Unit = {
    val rddWeather = sc.textFile("hdfs:/bigdata/dataset/weather-sample").map(WeatherData.extract)

    // Average temperature for every month
    rddWeather
      .filter(_.temperature<999)
      .map(x => (x.month, x.temperature))
      .aggregateByKey((0.0,0.0))((a,v)=>(a._1+v,a._2+1),(a1,a2)=>(a1._1+a2._1,a1._2+a2._2))
      .map({case(k,v)=>(k,v._1/v._2)})
      .collect()

    // Maximum temperature for every month
    rddWeather
      .filter(_.temperature<999)
      .map(x => (x.month, x.temperature))
      .reduceByKey((x,y)=>{if(x<y) y else x})
      .collect()
	
	val cachedRdd = rddWeather.coalesce(8).filter(_.temperature<999).map(x => (x.month, x.temperature)).cache()

	cachedRdd.aggregateByKey((0.0,0.0))((a,v)=>(a._1+v,a._2+1), (a1,a2)=>(a1._1+a2._1,a1._2+a2._2)).map({case(k,v)=>(k,v._1/v._2)}).collect()
	cachedRdd.reduceByKey((x,y)=>{if(x<y) y else x}).collect()
  }

  /**
   * Find the best option
   * @param sc
   */
  def exercise2(sc: SparkContext): Unit = {
    import org.apache.spark.HashPartitioner
    val p = new HashPartitioner(8)

    val rddStation = sc.textFile("hdfs:/bigdata/dataset/weather-info/stations.csv").map(StationData.extract)

    // val rddS1 = rddStation.partitionBy(p).keyBy(x => x.usaf + x.wban).cache()
    // val rddS2 = rddStation.partitionBy(p).cache().keyBy(x => x.usaf + x.wban)
    val rddS3 = rddStation.keyBy(x => x.usaf + x.wban).partitionBy(p).cache()
    // val rddS4 = rddStation.keyBy(x => x.usaf + x.wban).cache().partitionBy(p)

  }

  /**
   * Define the join between rddWeather and rddStation and compute:
   * - The maximum temperature for every city
   * - The maximum temperature for every city in Italy
   *   - StationData.country == "IT"
   * - Sort the results by descending temperature
   *   - map({case(k,v)=>(v,k)}) to invert key with value and vice versa
   *
   * Hints & considerations:
   * - Keep only temperature values <999
   * - Join syntax: rdd1.join(rdd2)
   * - Both RDDs should be structured as key-value RDDs with the same key: usaf + wban
   * - Consider partitioning and caching to optimize the join
   * - Careful: it is not enough for the two RDDs to have the same number of partitions;
   *   they must have the same partitioner!
   * - Verify the execution plan of the join in the web UI
   *
   * @param sc
   */
  def exercise3(sc: SparkContext): Unit = {
    val rddWeather = sc.textFile("hdfs:/bigdata/dataset/weather-sample").map(WeatherData.extract)
    val rddStation = sc.textFile("hdfs:/bigdata/dataset/weather-info/stations.csv").map(StationData.extract)

    import org.apache.spark.HashPartitioner

	val rddS = rddStation.keyBy(x => x.usaf + x.wban).partitionBy(new HashPartitioner(8))
	val rddW = rddWeather.filter(_.temperature<999).keyBy(x => x.usaf + x.wban).partitionBy(new HashPartitioner(8))

	val rddJoin = rddW.join(rddS)
	rddJoin.toDebugString
	rddJoin
	rddJoin.collect()

	cachedRdd.map({case(k,v)=>(v._2.name,v._1.temperature)}).reduceByKey((x,y)=>{if(x<y) y else x}).collect()
	cachedRdd.filter(_._1._2.country=="IT").map({case(k,v)=>(v._2.name,v._1.temperature)}).reduceByKey((x,y)=>{if(x<y) y else x}).collect()
	cachedRdd.filter(_._1._2.country=="IT").map({case(k,v)=>(v._2.name,v._1.temperature)}).reduceByKey((x,y)=>{if(x<y) y else x}).map({case(k,v)=>(v,k)}).sortByKey(false).collect()
  }

  /**
   * Use Spark's web UI to verify the space occupied by the following RDDs
   * @param sc
   */
  def exercise4(sc: SparkContext): Unit = {
    import org.apache.spark.storage.StorageLevel._
    val rddWeather = sc.textFile("hdfs:/bigdata/dataset/weather-sample").map(WeatherData.extract)

    sc.getPersistentRDDs.foreach(_._2.unpersist())

    val memRdd = rddWeather.sample(false,0.1).repartition(8).cache()
    val memSerRdd = memRdd.map(x=>x).persist(MEMORY_ONLY_SER)
    val diskRdd = memRdd.map(x=>x).persist(DISK_ONLY)

    memRdd.collect()
    memSerRdd.collect()
    diskRdd.collect()
  }

  /**
   * Consider the following scenario:
   * - We have a disposable RDD of Weather data (i.e., it is used only once): rddW
   * - And we have an RDD of Station data that is used many times: rddS
   * - Both RDDs are cached (collect() is called to enforce caching)
   *
   * We want to join the two RDDS. Which option is best?
   * - Simply join the two RDDs
   * - Enforce on rddW1 the same partitioner of rddS (and then join)
   * - Exploit broadcast variables
   * @param sc
   */
  def exercise5(sc: SparkContext): Unit = {
    import org.apache.spark.HashPartitioner

    val rddWeather = sc.textFile("hdfs:/bigdata/dataset/weather-sample").map(WeatherData.extract)
    val rddStation = sc.textFile("hdfs:/bigdata/dataset/weather-info/stations.csv").map(StationData.extract)

    val rddW = rddWeather
      .sample(false,0.1)
      .filter(_.temperature<999)
      .keyBy(x => x.usaf + x.wban)
      .cache()
    val rddS = rddStation
      .keyBy(x => x.usaf + x.wban)
      .partitionBy(new HashPartitioner(8))
      .cache()

    // Collect to enforce caching
    rddW.collect
    rddS.collect

    // Is it better to simply join the two RDDs..
    rddW
      .join(rddS)
      .filter(_._2._2.country=="IT")
      .map({case(k,v)=>(v._2.name,v._1.temperature)})
      .reduceByKey((x,y)=>{if(x<y) y else x})
      .collect()

    // ..to enforce on rddW1 the same partitioner of rddS..
    rddW
      .partitionBy(new HashPartitioner(8))
      .join(rddS)
      .filter(_._2._2.country=="IT")
      .map({case(k,v)=>(v._2.name,v._1.temperature)})
      .reduceByKey((x,y)=>{if(x<y) y else x})
      .collect()

    // ..or to exploit broadcast variables?
    val bRddS = sc.broadcast(rddS.collectAsMap())
    val rddJ = rddW
      .map({case (k,v) => (bRddS.value.get(k),v)})
      .filter(_._1!=None)
      .map({case(k,v)=>(k.get.asInstanceOf[StationData],v)})
    rddJ
      .filter(_._1.country=="IT")
      .map({case (k,v) => (k.name,v.temperature)})
      .reduceByKey((x,y)=>{if(x<y) y else x})
      .collect()
	  
	// Simple join: directly shuffles rddW to rddS's partitions
	// Partition forcing: slightly more expensive, as rddW partitions still need to be shuffled after applying partitioning criteria
	// Broadcast: is the most efficient, but it works only if the smaller dataset is very small
  }

}