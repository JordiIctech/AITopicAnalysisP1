import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object learning {
  def main(args: Array[String]): Unit = {
/*
    testing.connectlink()

    testing.agegroup()

    testing.yeargroup()

    testing.monthgroup()

    //-------------------------------------------------------------------------------------------------------------
    testing.diseasegroup("Heart_Disease") //Least amount of people go to the hospital during holidays.

    testing.diseasegroup("Chronic_Respiratory_Diseases")
    //-------------------------------------------------------------------------------------------------------------

    //testing.calcdiff()

    testing.mostvulnerable()

    testing.leastvulnerable()

    testing.costumvulnerable()

    //testing.generalquery()
*/
    testing.spark.close()
  }
}
