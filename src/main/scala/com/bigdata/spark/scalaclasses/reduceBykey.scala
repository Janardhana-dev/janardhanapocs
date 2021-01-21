package com.bigdata.spark.scalaclasses

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object reduceBykey {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("reduceBykey").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data = "D:\\Bigdata\\datasets\\asl.csv"
    val aslrdd = sc.textFile(data)
    val skip = aslrdd.first()
    val res = aslrdd.filter(x=>x!=skip).map(x=>x.split(",")).map(x=>(x(2),1)).reduceByKey((a,b)=>a+b)
    res.collect.foreach(println)
    spark.stop()
  }
}