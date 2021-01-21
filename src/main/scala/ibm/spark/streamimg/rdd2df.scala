package ibm.spark.streamimg

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
// now rdd convert to df using programmatically specify schema //
object rdd2df {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("rdd2df").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data = "D:\\Bigdata\\datasets\\bank-full.csv"
    // 4 spayears back old strategy
    val rdd=sc.textFile(data)
    val head=rdd.first()
    val fields = head.split(";").map(x => StructField(x.replaceAll("\"",""), StringType, nullable = true))
    val schema = StructType(fields)
    // Convert records of the RDD (people) to Rows
    val rowRDD = rdd.filter(x=>x!=head).map(x=>x.replaceAll("\"","").split(";")).map(x => Row.fromSeq(x))
    // Apply the schema to the RDD
    val df = spark.createDataFrame(rowRDD, schema)
    df.show(5)
    spark.stop()
  }
}
