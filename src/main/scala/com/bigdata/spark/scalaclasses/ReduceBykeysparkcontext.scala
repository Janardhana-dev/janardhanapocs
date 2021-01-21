package com.bigdata.spark.scalaclasses

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ReduceBykeysparkcontext {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("ReduceBykeysparkcontext").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    //val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data = "D:\\Bigdata\\datasets\\donations.txt"
    val output = "D:\\Bigdata\\datasets\\output\\donationop"
    val sc = spark.sparkContext
    val rdd= sc.textFile(data,6)
    val head = rdd.first()
    val res = rdd.filter(x=>x!=head).map(x=>x.split(",")).map(x=>(x(0),x(1).toInt)).reduceByKey((a,b)=>a+b,1).sortBy(x=>x._2,false).toDF("name","amt")
    res.write.format("csv").option("header","true").save(output)
   // res.collect.foreach(println)
    //res.saveAsTextFile(output)
    spark.stop()
  }
}