package ibm.spark.streamimg

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object complexjsondata {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("complexjsondata").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data="D:\\Bigdata\\datasets\\zips.json"
    val df=spark.read.format("json").load(data)
    df.createOrReplaceTempView("tab")
    df.printSchema()
//    val qry="select state,city from tab group by state,city order by city asc"
   // val qry="select _id id,city,abs(loc[0]) lang,loc[1] lati,pop,state from tab where state='NJ'"
   // val res=spark.sql(qry)
    // withColumn used to create new column if columns not exists, if column exists its update column
    val res = df.withColumn("age",lit("TESTING")).withColumn("city", when($"city"==="BLANDFORD","BLA").otherwise($"city"))
      .withColumn("state",regexp_replace($"state","MA","MAHARASTRA"))
    res.printSchema()
    res.show(6)
    spark.stop()
  }
}