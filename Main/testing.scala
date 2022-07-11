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

  spark.sql("DROP table IF EXISTS MortalityDatabase")
  spark.sql("create table IF NOT EXISTS MortalityDatabase(Date String, DateStart String, DateEnd String, Jurisdiction String, " +
    "Year INT, MONTH INT, Race String, Age String, All_Deaths INT, Natural INT, Septicimeia INT, Malignant INT, Neoplasms INT, " +
    "Diabetes INT, Alzheimer INT, Influenza_Pneumonia INT, Chronic_Respiratory_Diseases	INT, Respiratory_System INT,	Nephritis INT, "+
    "UnClassified INT, Heart_Disease INT, Cerebrovascular_Diseases INT, COVID19_Others INT,	COVID19 INT) " +
    "row format delimited fields terminated by ','") //TBLPROPERTIES('skip.header.line.count'='1')
  spark.sql("LOAD DATA LOCAL INPATH 'MortalityDatabase.csv' INTO TABLE MortalityDatabase")

  spark.sql("DROP table IF EXISTS UserInfo")
  spark.sql("create table IF NOT EXISTS UserInfo(Username String, Password String) row format delimited fields terminated by ','")
  spark.sql("LOAD DATA LOCAL INPATH 'userinfo.csv' INTO TABLE UserInfo")

  //spark.sql()
  spark.sql("SELECT * FROM UserInfo").show()

  //spark.sql("ALTER TABLE UserInfo.Password WHERE UserInfo.Username = 'person2' RENAME TO 'passwords2'").show()

  def yearmin(): Unit ={
    spark.sql("SELECT Age, SUM(All_Deaths) AS Msum FROM MortalityDatabase WHERE Year=2020 GROUP BY Age ORDER BY CAST(Msum as int)").show()
  }

}