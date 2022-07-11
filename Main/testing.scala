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
  def all2021column(currentselect: String)= {
    var selection = currentselect
    spark.sql(f"SELECT $selection FROM MortalityDatabase WHERE MortalityDatabase.Year='2021'").show() //Shows all select column for year 2021
  }

  //-------------------------------------------------------------------------------------------------------------------

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

  def diseasegroup(selecteddisease: String): Unit ={ //
    spark.sql(s"SELECT Month, ROUND(AVG($selecteddisease),2) as Msum FROM MortalityDatabase GROUP BY Month ORDER BY CAST(Msum as int)").show
  }
  //-------------------------------------------------------------------------------------------------------------------
  /*
  def calcdiff(): Unit ={ //
    spark.sql("SELECT Year, Month, Age, Race, LAG(All_Deaths) " +
      "AS previous_month_total, previous_month_total - LAG(previous_month_total) OVER (ORDER BY Month ) " +
      "AS difference_previous_month FROM MortalityDatabase WHERE Age LIKE '%45-54%' AND Race='Hispanic' ORDER BY Month").show
  }
  //-------------------------------------------------------------------------------------------------------------------
 */
  def generalquery(): Unit ={
    spark.sql("SELECT * FROM MortalityDatabase WHERE Age LIKE '%85%' AND Month='12'").show(false)
  }

  //-------------------------------------------------------------------------------------------------------------------
  def mostvulnerable(): Unit ={ //Deathrates increased by over 15% overall in 2020 compared to 2019.
    spark.sql("SELECT * FROM MortalityDatabase WHERE Age LIKE '%85%' AND Month='12'").show(false)

    spark.sql("SELECT Year, SUM(All_Deaths), SUM(Natural), SUM(Septicimeia), SUM(Neoplasms), SUM(Diabetes), " +
      "SUM(Alzheimer), SUM(Influenza_Pneumonia), SUM(Chronic_Respiratory_Diseases), SUM(Respiratory_System), SUM(Nephritis)," +
      "SUM(UnClassified), SUM(Heart_Disease), SUM(Cerebrovascular_Diseases), SUM(COVID19_Others), SUM(COVID19) " +
      "FROM MortalityDatabase WHERE Age LIKE '%85%' AND Month='12' GROUP BY Year").show(false)
  }
  //-------------------------------------------------------------------------------------------------------------------
  def leastvulnerable(): Unit ={ //Covid hit kids soft compared to elders.
    spark.sql("SELECT * FROM MortalityDatabase WHERE Age LIKE '%14%' AND Month='12'").show(false)

    spark.sql("SELECT Year, SUM(All_Deaths), SUM(Natural), SUM(Septicimeia), SUM(Neoplasms), SUM(Diabetes), " +
      "SUM(Alzheimer), SUM(Influenza_Pneumonia), SUM(Chronic_Respiratory_Diseases), SUM(Respiratory_System), SUM(Nephritis)," +
      "SUM(UnClassified), SUM(Heart_Disease), SUM(Cerebrovascular_Diseases), SUM(COVID19_Others), SUM(COVID19) " +
      "FROM MortalityDatabase WHERE Age LIKE '%14%' AND Month='12' GROUP BY Year").show(false)
  }
  //-------------------------------------------------------------------------------------------------------------------
  def costumvulnerable(): Unit ={
    spark.sql("SELECT * FROM MortalityDatabase WHERE Age LIKE '%"+ "14" + "%' AND Month='12'").show(false)

    spark.sql("SELECT Year, SUM(All_Deaths), SUM(Natural), SUM(Septicimeia), SUM(Neoplasms), SUM(Diabetes), " +
      "SUM(Alzheimer), SUM(Influenza_Pneumonia), SUM(Chronic_Respiratory_Diseases), SUM(Respiratory_System), SUM(Nephritis)," +
      "SUM(UnClassified), SUM(Heart_Disease), SUM(Cerebrovascular_Diseases), SUM(COVID19_Others), SUM(COVID19) " +
      "FROM MortalityDatabase WHERE Age LIKE '%14%' AND Month='12' GROUP BY Year").show(false)
  }

  //Delete row where username is then insert a new one with the new username.
}