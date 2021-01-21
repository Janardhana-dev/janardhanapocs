package ibm.spark.streamimg

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import ibm.spark.streamimg.importall._

object COMPLEXJSONDATA2 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("COMPLEXJSONDATA2").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val data="D:\\Bigdata\\datasets\\world_bank.json"
    val df=spark.read.format("json").load(data)
    // data cleaning process
    // json ... mobiles, cloud , social media, most of servers generate json format. light weight format and support any datatypes.
    //struct .... if u have struct parent_column.child_column
    val res = df.withColumn("theme1name",$"theme1.Name").withColumn("theme1percent",$"theme1.Percent")
      .drop($"theme1").withColumn("theme_namecode", explode(col("theme_namecode")))
      .withColumn("sector_namecode", explode(col("sector_namecode")))
      .withColumn("sector", explode(col("sector")))
      .withColumn("mjsector_namecode", explode(col("mjsector_namecode")))
      .withColumn("majorsector_percent", explode(col("majorsector_percent")))
      .withColumn("projectdocs", explode(col("projectdocs")))
      .withColumn("mjtheme_namecode", explode(col("mjtheme_namecode")))
      .withColumn("idoid",$"_id.$$oid")

    // explode remove array from array(struct type
    val res1 = res.select($"*",$"projectdocs.*",$"project_abstract.*",$"theme_namecode.name".alias("theme_namecodename"),$"theme_namecode.code".alias("theme_namecodecode"),$"sector_namecode.code".alias("sector_namecodecode"),$"sector_namecode.name".alias("sector_namecodename"),
      $"sector4.Name".alias("sector4name"),$"sector4.Percent".alias("sector4percent"),
      $"sector1.Name".alias("sector1name"),$"sector1.Percent".alias("sector1percent"),
      $"sector2.Name".alias("sector2name"),$"sector2.Percent".alias("sector2percent"),
      $"sector.Name".alias("sectorname"),$"project_abstract.cdata".alias("project_abstractcdata"),
      $"mjtheme_namecode.code".alias("mjtheme_namecodecode"),$"mjtheme_namecode.name".alias("mjtheme_namecodename"),
      $"sector3.Name".alias("sector3name"),$"sector3.Percent".alias("sector3percent"),
      $"mjsector_namecode.code".alias("mjsector_namecodecode"),$"mjsector_namecode.name".alias("mjsector_namecodename"),
      $"majorsector_percent.Name".alias("majorsector_percentname"),$"majorsector_percent.Percent".alias("majorsector_percentpercent"),
      explode($"mjtheme").alias("mjthemedata")
    )

      .drop("mjtheme","_id","projectdocs","theme_namecode","project_abstract","mjsector_namecode","mjtheme_namecode","sector_namecode","sector4","sector3","sector2","sector1","sector","majorsector_percent")
    res1.show(9,false)
    res1.printSchema()
    // data cleaning processing completed.
    //export data to oracle/mysql /mssql
//    res1.write.jdbc(msurl,"jsondata_jana_t1",msprop)
//    println("mssql completed")
    res1.write.jdbc(ourl,"jsonora_jana_t1",oprop)
    println("orcl completed")
    spark.stop()

  }
}
