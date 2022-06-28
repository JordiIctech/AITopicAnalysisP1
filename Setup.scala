import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Setup{ // Object file name.
  def main(args: Array[String]): Unit = {             //Need hadoop, scala2.13, hive and jdk8 installed.
    // create a spark session
    // for Windows
    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    println("Created spark session.")
    /*Anything above this has been mostly unchanged (generalized)*/

    val df2 = spark.read.format("csv").option("sep",",").load("hdfs://localhost:9000/user/hive/warehouse/Bev_BranchA.txt") // File location
    df2.createOrReplaceTempView("beverage")                       //Type didn't seem to matter, just loaded straight .txt that looked like tables.
    spark.sql("SELECT * FROM beverage;").show()                     // Show() displays current tables.
    spark.sql("SELECT _c0 from beverage WHERE _c1='Branch6'").show() //Shows all tables that have Branch 6

  }
}