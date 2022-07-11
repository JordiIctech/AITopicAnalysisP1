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
  //-------------------------------------------------------------------------------------------------------------------
  def connectlink(): Unit ={
    println("Status----------------->Connected")
  }
  //-------------------------------------------------------------------------------------------------------------------
  def all2021column(currentselect: String)= {
    var selection = currentselect
    spark.sql(f"SELECT $selection FROM MortalityDatabase WHERE MortalityDatabase.Year='2021'").show() //Shows all select column for year 2021
  }
  //-------------------------------------------------------------------------------------------------------------------
  def passwordmanagement() ={
    var currentselect = scala.io.StdIn.readLine() //Extract password based on given username
    var extractpass = spark.sql(s"SELECT Password FROM UserInfo WHERE UserInfo.Username = '$currentselect'").first()

    var currentpass = extractpass(0)

    if( currentpass == "bigboss" ){
      println("Pass 1 Selected")
    } else if( currentpass == "word2" ){
      println("Pass 3 Selected")
    }
    else{
      println("No Pass Selected")}
    }
  //-------------------------------------------------------------------------------------------------------------------



  //Need function to add disease column

}
