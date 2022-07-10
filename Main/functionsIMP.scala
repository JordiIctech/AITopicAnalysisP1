import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

object functionsIMP {
  def main(args: Array[String]): Unit = {

  }  //Functions must be outside of the main function.
  System.setProperty("hadoop.home.dir", "C:\\hadoop")
  val spark = SparkSession
    .builder
    .appName("hello hive")
    .config("spark.master", "local[*]")
    .enableHiveSupport()
    .getOrCreate()
  Logger.getLogger("org").setLevel(Level.ERROR)
  println("Status----------------->Spark Session Created")
  spark.sparkContext.setLogLevel("ERROR")

  def all2021column(currentselect: String)= {
    var selection = currentselect
    spark.sql(f"SELECT $selection FROM MortalityDatabase WHERE MortalityDatabase.Year='2021'").show() //Shows all select column for year 2021
  }

  def connectlink(): Unit ={
    println("Status----------------->Connected")
  }

}
