//import scala.util.Random
//
//import org.apache.spark.sql.SparkSession
//
//val spark = SparkSession.builder().getOrCreate()
//val sc = spark.sparkContext
//
////val ipsNo = 1000000
////val devsNo = 10000000
//val ipsNo = 10
//val devsNo = 100
//
//val ipsSizes = 1.to(ipsNo).map(i => Random.nextInt(25))
//val ipsDevs = sc.parallelize(ipsSizes).map(size => 1.to(size).map(d => Random.nextInt(devsNo).toLong).toList)
//
