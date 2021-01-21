package com.bigdata.spark.scalaclasses

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object flatmap {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("flatmap").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data ="D:\\Bigdata\\datasets\\wcdata.txt"
    val urdd=sc.textFile(data)
    val res=urdd.flatMap(x=>x.split(" "))
    res.take(2).foreach(println)
   // urdd.collect().foreach(println)
    spark.stop()
  }
}