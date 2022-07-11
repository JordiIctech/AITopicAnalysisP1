import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object mainUA {
  def main(args: Array[String]): Unit = {

    functionsIMP.connectlink()
    var running: String = "True"

    while(running != "End") {
      println("What do you wish to do?")
      running = scala.io.StdIn.readLine()
      println("Status----------------->" + running + " Selected, Loading Process")

      if (running=="1"){
        println("What is your username?")
        functionsIMP.passwordmanagement("admin") //Input username and check for password. scala.io.StdIn.readLine()
      }

      if (running=="2") {
        println("What column do you wish to see?")
        functionsIMP.all2021column("All_Deaths") // First one has to ask prompt after connecting.   scala.io.StdIn.readLine()
      }

      if (running=="3") {
        functionsIMP.connectlink()
      }

      if (running=="4") {
        functionsIMP.agegroup()
      }

      if (running=="5") {
        functionsIMP.yeargroup()
      }

      if (running=="6") {
        functionsIMP.monthgroup()
      }

      if (running=="7") {

        functionsIMP.diseasegroup("Heart_Disease") //Least amount of people go to the hospital during holidays.
        functionsIMP.diseasegroup("Chronic_Respiratory_Diseases")
      }

      //functionsIMP.calcdiff()
      if (running=="8") {
        functionsIMP.mostvulnerable()
      }

      if (running=="9") {
        functionsIMP.leastvulnerable()
      }

      if (running=="10") {
        functionsIMP.costumvulnerable()
      }

      //functionsIMP.generalquery()
      if (running == "End"){
        println("Status----------------->Closing Application")
      }
    }
    functionsIMP.spark.close()
  }
}
