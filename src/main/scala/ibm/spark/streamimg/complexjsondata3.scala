package ibm.spark.streamimg

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object complexjsondata3 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("complexjsondata3").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val data="D:\\Bigdata\\datasets\\companies.json"
    val df=spark.read.format("json").load(data)
//    val res = df.withColumn(

    df.show()
    spark.stop()
  }
}
