import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object createtables {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    println("created spark session")
    spark.sparkContext.setLogLevel("ERROR")


    spark.sql("SELECT * FROM MortalityDatabase").show()
  }
}
