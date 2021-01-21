package ibm.spark.streamimg

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object csvdata {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("csvdata").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
//    val data = "D:\\Bigdata\\datasets\\us-500.csv"
    val data = args(0)
    val df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
    df.createOrReplaceTempView("tab")
    val res= spark.sql("select * from tab where state='NY'")
    res.show(5)
    val tab = args(1)
    val msurl="jdbc:sqlserver://mdabdensbt packagemssql.ck6vgv6qae1n.us-east-2.rds.amazonaws.com:1433;databaseName=rafidb;"
    val msprop = new java.util.Properties()
    msprop.setProperty("user","msuername")
    msprop.setProperty("password","mspassword")
    msprop.setProperty("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
    res.write.jdbc(msurl,tab,msprop)
    res.printSchema()
    spark.stop()
  }
}