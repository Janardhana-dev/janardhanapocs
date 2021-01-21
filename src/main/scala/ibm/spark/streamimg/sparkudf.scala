package ibm.spark.streamimg

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object sparkudf {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("sparkudf").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
//    val data = "D:\\Bigdata\\datasets\\bank-full.csv"
val data = "D:\\Bigdata\\datasets\\us-500.csv"
    val df = spark.read.format("csv").option("header","true").option("delimiter",",").load(data)
    //df.show()
    df.createOrReplaceTempView("tab")
    // val res = spark.sql("select * from tab where balance>70000 and marital='married'")
    //val res = df.where($"balance">=60000 && $"marital"==="married") //domain specific language
    //val res = spark.sql("select *, concat(job,'',marital,'',education) fullname, concat_ws(' ',job,marital,education) fullname1 from tab")
//    val res = df.withColumn("fullname", concat_ws("_",$"marital",$"job",$"education"))
//      .withColumn("fullname1",concat($"marital", lit("-"),$"job", lit("_"),$"education"))

// val res=df.withColumn("job",regexp_replace($"job","-",""))
//     .when($"marital"==="divorced","separated").otherwise($"marital"))
//   .withColumn("marital",when($"marital"==="married","couple")
//     .when($"marital"==="single","bachelor")
//    val res=df.groupBy($"marital").agg(count("*").alias("cnt")).orderBy($"cnt".desc) // aggregator function
//  val res = df.groupBy($"state",$"city").agg(collect_list($"first_name").alias("names")
//  val res = df.groupBy($"state",$"city").agg(collect_list($"first_name").alias("names"))

    /*    val offer = (state:String)=> state match {
      case "OH"=>"20% off"
      case "NJ" |"CA"=>"10% off"
      case "NY" | "MI"| "IL"=> "30% off"
      case _ => "no offer"
    }*/
    def offer (state:String)= state match {
      case "OH"=>"20% off"
      case "NJ" |"CA"=>"10% off"
      case "NY" | "MI"| "IL"=> "30% off"
      case _ => "no offer"
    }

    val uf = udf(offer _) //spark don't know anything but support udf.. convert function to udf
    val res = df.withColumn("weekendoffer",uf($"state"))
    res.show(15,false)
    res.printSchema()


    spark.stop()
  }
}