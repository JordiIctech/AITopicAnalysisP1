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

    spark.sql("DROP table IF EXISTS MortalityDatabase")
    spark.sql("create table IF NOT EXISTS MortalityDatabase(Date String, DateStart String, DateEnd String, Jurisdiction String, " +
      "Year INT, Month INT, Race String, Age String, All_Deaths INT, Natural INT, Septicimeia INT, Neoplasms INT, " +
      "Diabetes INT, Alzheimer INT, Influenza_Pneumonia INT, Chronic_Respiratory_Diseases	INT, Respiratory_System INT,	Nephritis INT, "+
      "UnClassified INT, Heart_Disease INT, Cerebrovascular_Diseases INT, COVID19_Others INT,	COVID19 INT) " +
      "row format delimited fields terminated by ','") //TBLPROPERTIES('skip.header.line.count'='1')
    spark.sql("LOAD DATA LOCAL INPATH 'MortalityDatabase.csv' INTO TABLE MortalityDatabase")

    spark.sql("DROP table IF EXISTS UserInfo")
    spark.sql("create table IF NOT EXISTS UserInfo(Username String, Password String) row format delimited fields terminated by ','")
    spark.sql("LOAD DATA LOCAL INPATH 'userinfo.csv' INTO TABLE UserInfo")

    spark.sql("SELECT * FROM MortalityDatabase").show()
    spark.sql("SELECT * FROM UserInfo").show

    spark.sql("DROP TABLE IF EXISTS HeartMortality")
    spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("create table IF NOT EXISTS HeartMortality(Year Int, Month Int, Heart_Disease String) " +
      "PARTITIONED by (AgeGroup String) row format delimited fields terminated by ',' ")
    spark.sql("INSERT INTO TABLE HeartMortality(SELECT MortalityDatabase.Year, MortalityDatabase.Month, MortalityDatabase.Heart_Disease, " +
      " MortalityDatabase.Age FROM MortalityDatabase WHERE Age='0-4 years' or Age='5-14 years' or Age='15-24 years') ") //Partitioned children
    spark.sql("SELECT * FROM HeartMortality").show
    spark.sql("SHOW PARTITIONS HeartMortality").show()


    spark.close()
  }
}
