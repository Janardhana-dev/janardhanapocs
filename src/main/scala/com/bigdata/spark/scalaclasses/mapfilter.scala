package com.bigdata.spark.scalaclasses

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object mapfilter {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("mapfilter").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data = "D:\\Bigdata\\datasets\\asl.csv"
    val aslrdd = sc.textFile(data)
    //select * from tab where city=...
    //select city, count(*) from tab group by city
    //select join two tables
    // val res = aslrdd.filter(x=>x.contains("blr"))
    val skip = aslrdd.first()
    val res = aslrdd.filter(x=>x!=skip).map(x=>x.split(",")).map(x=>(x(0),x(1).toInt,x(2))).filter(x=>x._3=="blr" && x._2<40)
      //array(jyo,12,blr,fri)
    //array(koti,29,blr,saturday)
    res.collect.foreach(println)
    spark.stop()
  }
}