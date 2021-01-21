package ibm.spark.streamimg

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object df2_emailcount {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("df2_emailcount").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data = "D:\\Bigdata\\datasets\\us-500.csv"
    //val rdd=sc.textFile(data)
   // val head=rdd.first()
   // val fields = head.split(";").map(x => StructField(x.replaceAll("\"",""), StringType, nullable = true))
   // val schema = StructType(fields)
    // Convert records of the RDD (people) to Rows
   // val rowRDD = rdd.filter(x=>x!=head).map(x=>x.replaceAll("\"","").split(";")).map(x => Row.fromSeq(x))
    // Apply the schema to the RDD
    val df = spark.read.format("csv").option("header","true").option("delimiter",",")
     .option("inferSchema","true").load(data)
    df.createOrReplaceTempView("tab")
    val res = spark.sql("select (SUBSTRING_INDEX(substring(email, INSTR(email,'@') + 1),'.',1)) domain_name,count(*) count from tab group by domain_name order by count desc")
    res.show()
    spark.stop()

  }
}
