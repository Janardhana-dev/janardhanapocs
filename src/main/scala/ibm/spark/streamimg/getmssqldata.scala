package ibm.spark.streamimg

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object getmssqldata {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("getmssqldata").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    //one way to get data from any db
    val qry = "(select * from abhidf where sal>2500) abc "
    val url = "jdbc:sqlserver://mdabdenmssql.ck6vgv6qae1n.us-east-2.rds.amazonaws.com:1433;databaseName=rafidb;"
    val df = spark.read.format("jdbc").option("user","msuername")
      .option("password","mspassword")
      .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver").option("dbtable","abhidf").option("url",url).load()
 //   df.show()
 //   spark.stop()

    //recommended for scala/java
    import java.util.Properties
    //second way to get data from db
    val msprop = new Properties()
    msprop.setProperty("user","msuername")
    msprop.setProperty("password","mspassword")
    msprop.setProperty("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
    val msdf = spark.read.jdbc(url,qry,msprop)
    // second way
    val ourl ="jdbc:oracle:thin:@//oracledb.c1zbxkbn0gw7.us-east-1.rds.amazonaws.com:1521/ORCL"
    import java.util.Properties
    val oprop = new Properties()
    oprop.setProperty("user","ousername")
    oprop.setProperty("password","opassword")
    oprop.setProperty("driver","oracle.jdbc.driver.OracleDriver")
    val odf = spark.read.jdbc(ourl,"EMP",oprop)
    odf.show()
    msdf.createOrReplaceTempView("ms")
    odf.createOrReplaceTempView("o")
    val join = spark.sql("select o.*,ms.loc,ms.dname from ms join o on o.deptno=ms.deptno")
    join.show()

    //Exception in thread "main" java.lang.ClassNotFoundException: com.microsoft.sqlserver.jdbc.SQLServerDriver
    // if u get above error there is dependency problem. add mssql jdbc jar manually file> project structure> dependencies>+ > jars...+ add dependencies > ok
    val murl ="jdbc:mysql://mysqldb.c1zbxkbn0gw7.us-east-1.rds.amazonaws.com:3306/mysqldb"
    //import java.util.Properties
    val mprop = new Properties()
    mprop.setProperty("user","myusername")
    mprop.setProperty("password","mypassword")
    mprop.setProperty("driver","com.mysql.jdbc.Driver")
//    join.write.mode(SaveMode.Overwrite).jdbc(murl,"mssqlorajointab",mprop)
    join.write.mode(SaveMode.Overwrite).jdbc(murl, "mssqlorajointab",mprop)
    spark.stop()
  }
}
