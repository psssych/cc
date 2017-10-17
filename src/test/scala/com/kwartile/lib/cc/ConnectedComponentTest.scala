package com.kwartile.lib.cc

import org.apache.spark.rdd.RDD
import org.scalatest.{FlatSpec, Matchers}
import org.apache.spark.{SparkContext, SparkConf}

class ConnectedComponentTest extends FlatSpec with Matchers {
  val sparkConf = new SparkConf()
    .setAppName("ConnectedComponent")
    .setMaster("local[*]")
  def sc = SparkContext.getOrCreate(sparkConf)

  // behavior of "PII"

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
}