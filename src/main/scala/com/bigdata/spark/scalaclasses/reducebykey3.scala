package com.bigdata.spark.scalaclasses

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object reducebykey3 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("reducebykey3").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data = "D:\\Bigdata\\datasets\\asl.csv"
    val aslrdd = sc.textFile(data)
    val skip = aslrdd.first()
    //toDF method used convert structure rdd to dataframe and to rename column
    val res = aslrdd.filter(x=>x!=skip).map(x=>x.split(",")).map(x=>(x(0),x(1),x(2))).toDF("name","age","city")
    res.createOrReplaceTempView("tab")
   val result = spark.sql("select city, count(*) cnt from tab group by city order by cnt desc")
    result.show()
    spark.stop()
  }
}