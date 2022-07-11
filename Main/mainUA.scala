import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object mainUA {
  def main(args: Array[String]): Unit = {

    functionsIMP.connectlink()

    println("What is your username?")
    functionsIMP.passwordmanagement("admin") //Input username and check for password. scala.io.StdIn.readLine()

    println("What column do you wish to see?")
    functionsIMP.all2021column("All_Deaths") // First one has to ask prompt after connecting.   scala.io.StdIn.readLine()

    functionsIMP.connectlink()

    functionsIMP.agegroup()

    functionsIMP.yeargroup()

    functionsIMP.monthgroup()

    //-------------------------------------------------------------------------------------------------------------
    functionsIMP.diseasegroup("Heart_Disease") //Least amount of people go to the hospital during holidays.

    functionsIMP.diseasegroup("Chronic_Respiratory_Diseases")
    //-------------------------------------------------------------------------------------------------------------

    //functionsIMP.calcdiff()

    functionsIMP.mostvulnerable()

    functionsIMP.leastvulnerable()

    functionsIMP.costumvulnerable()

    //functionsIMP.generalquery()

    functionsIMP.spark.close()
  }
}
