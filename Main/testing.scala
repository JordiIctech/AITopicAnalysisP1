import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

object testing {
  def main(args: Array[String]): Unit = {
    // create a spark session
    // for Windows

  }
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

  def connectlink(): Unit ={
    println("Status----------------->Connected")
  }
  //-------------------------------------------------------------------------------------------------------------------
  //spark.sql("ALTER TABLE UserInfo.Password WHERE UserInfo.Username = 'person2' RENAME TO 'passwords2'").show()

  def agegroup(): Unit ={
    spark.sql("SELECT Age, SUM(All_Deaths) AS Msum FROM MortalityDatabase WHERE Year=2020 GROUP BY Age ORDER BY CAST(Msum as int)").show()
  }
  //-------------------------------------------------------------------------------------------------------------------
  def yeargroup(): Unit ={ //Average each month per year.
    spark.sql("SELECT Year, ROUND(AVG(All_Deaths),2) as Msum FROM MortalityDatabase GROUP BY Year ORDER BY CAST(Year as int)").show
  }
  //-------------------------------------------------------------------------------------------------------------------
  def monthgroup(): Unit ={ //Average deaths per month over the years
    spark.sql("SELECT Month, ROUND(AVG(All_Deaths),2) as Msum FROM MortalityDatabase GROUP BY Month ORDER BY CAST(Msum as int)").show
  }
  //-------------------------------------------------------------------------------------------------------------------
  /*
  def monthgroup(): Unit ={ //Average deaths per month over the years
    spark.sql("SELECT Month, ROUND(AVG(All_Deaths),2) as Msum FROM MortalityDatabase GROUP BY Month ORDER BY CAST(Msum as int)").show
  }
  //-------------------------------------------------------------------------------------------------------------------
  def monthgroup(): Unit ={ //Average deaths per month over the years
    spark.sql("SELECT Month, ROUND(AVG(All_Deaths),2) as Msum FROM MortalityDatabase GROUP BY Month ORDER BY CAST(Msum as int)").show
  }
  //-------------------------------------------------------------------------------------------------------------------
  def monthgroup(): Unit ={ //Average deaths per month over the years
    spark.sql("SELECT Month, ROUND(AVG(All_Deaths),2) as Msum FROM MortalityDatabase GROUP BY Month ORDER BY CAST(Msum as int)").show
  }
*/
  //Delete row where username is then insert a new one with the new username.
}