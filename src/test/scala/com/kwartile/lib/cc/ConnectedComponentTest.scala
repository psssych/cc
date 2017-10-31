package com.kwartile.lib.cc

import org.apache.spark.rdd.RDD
import org.scalatest.{FlatSpec, Matchers}
import org.apache.spark.{SparkContext, SparkConf}
import scala.util.Random
import org.apache.spark.storage.StorageLevel._

class ConnectedComponentTest extends FlatSpec with Matchers {
  val sparkConf = new SparkConf()
    .setAppName("ConnectedComponent")
    .setMaster("local[*]")
  def sc = SparkContext.getOrCreate(sparkConf)

  behavior of "HUJ"

  val cliques = sc.parallelize(
    List(
      List(1L, 2L, 3L),
      List(2L, 5L, 6L),
      List(1L, 2L),
      List(8L, 9L)
    )
  )

  it should "pass simple test for ConnectedComponents" in {
    val res =
      ConnectedComponent.run(cliques, 999)._1.map(x => x.swap).map(x => (x._1, Set(x._2))).reduceByKey(_ union _).map(_._2).collect

    res should contain only(Set(1L, 2L, 3L, 5L, 6L), Set(8, 9))
  }

  it should "pass simple test for ConnCompPsych" in {
    val res =
      ConnCompPsych.run(cliques, 999)._1.map(x => x.swap).map(x => (x._1, Set(x._2))).reduceByKey(_ union _).map(_._2).collect

    res should contain only(Set(1L, 2L, 3L, 5L, 6L), Set(8, 9))
  }

  it should "do nothing" in {
    //val ipsNo = 1000000
    //val devsNo = 10000000
    //val maxIpSize = 25
    val ipsNo = 10
    val devsNo = 100
    val maxIpSize = 3

    val ipsSizes = 1.to(ipsNo).map(i => Random.nextInt(maxIpSize) + 1)
    val ipsDevs = sc.parallelize(ipsSizes).map(size => 1.to(size).map(d => Random.nextInt(devsNo).toLong).toList).persist(MEMORY_AND_DISK_SER)


    val res = ConnCompPsych.run(ipsDevs, 999)._1.map(x => x.swap).map(x => (x._1, Set(x._2))).reduceByKey(_ union _).map(_._2).collect
    val res2 = ConnectedComponent.run(ipsDevs, 999)._1.map(x => x.swap).map(x => (x._1, Set(x._2))).reduceByKey(_ union _).map(_._2).collect

    println("Input: ")
    ipsDevs.collect().foreach(x => println(s"\t $x"))
    println("Results: ")
    res.foreach(x => println(s"\t ${x.toList.sorted}"))

    println("Results 2: ")
    res2.foreach(x => println(s"\t ${x.toList.sorted}"))

    res should contain only(res2:_*)
  }
}