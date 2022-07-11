import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object mainUA {
  def main(args: Array[String]): Unit = {

    functionsIMP.connectlink()
    var running: String = "True"
    var logginstatus: String = "Offline"
    var adminstatus: String = "False"
    var backbot: String = "On"

    while(running != "end") {
      println("=======================================================================================================")
      println("What do you wish to do?" +
        "\n1-Login/Switch Accounts" +
        "\n2" +
        "\n3" +
        "\n4" +
        "\n5" +
        "\n6" +
        "\n7" +
        "\n8" +
        "\n9" +
        "\n10" +
        "\n11" +
        "\n\t11a" +
        "\nEnd")
      running = scala.io.StdIn.readLine()
      println(Console.YELLOW + "Status----------------->" + running + " Selected, Loading Process" + Console.RESET)

      if (running=="1"){
        println("What is your username?")
        var inputusername: String = scala.io.StdIn.readLine()
        var confirmation = functionsIMP.passwordmanagement(inputusername)

        if (inputusername == "admin" && confirmation == "Correct"){
          logginstatus = "User"
          adminstatus = "True"
        }
        if (confirmation == "Correct"){
          logginstatus = "User"
        }
        backbot = scala.io.StdIn.readLine()
      }

      else if (running=="2" && logginstatus=="User") {
        println("What column do you wish to see?")
        functionsIMP.all2021column("All_Deaths") // First one has to ask prompt after connecting.   scala.io.StdIn.readLine()
        backbot = scala.io.StdIn.readLine()
      }

      else if (running=="3" && logginstatus=="User") {
        functionsIMP.connectlink()
        backbot = scala.io.StdIn.readLine()
      }

      else if (running=="4" && logginstatus=="User") {
        functionsIMP.agegroup()
        backbot = scala.io.StdIn.readLine()
      }

      else if (running=="5" && logginstatus=="User") {
        functionsIMP.yeargroup()
        backbot = scala.io.StdIn.readLine()
      }

      else if (running=="6" && logginstatus=="User") {
        functionsIMP.monthgroup()
        backbot = scala.io.StdIn.readLine()
      }

      else if (running=="7" && logginstatus=="User") {
        functionsIMP.diseasegroup("Heart_Disease") //Least amount of people go to the hospital during holidays.
        functionsIMP.diseasegroup("Chronic_Respiratory_Diseases")
        backbot = scala.io.StdIn.readLine()
      }

      //functionsIMP.calcdiff()
      else if (running=="8" && logginstatus=="User") {
        functionsIMP.mostvulnerable()
        backbot = scala.io.StdIn.readLine()
      }

      else if (running=="9" && logginstatus=="User") {
        functionsIMP.leastvulnerable()
        backbot = scala.io.StdIn.readLine()
      }

      else if (running=="10" && logginstatus=="User") {
        functionsIMP.costumvulnerable()
        backbot = scala.io.StdIn.readLine()
      }

      else if (running == "11" && adminstatus=="True"){
        println("Custom")
        backbot = scala.io.StdIn.readLine()
        //functionsIMP.generalquery()
      }

      else if (running == "end"){
        println(Console.RED + "Status----------------->Closing Application" + Console.RESET)
      }

      else{
        println(Console.RED + "Warning----------------->Insufficient Permissions For Requested Action<-----------------Warning" + Console.RESET)
      }
    }
    functionsIMP.spark.close()
  }
}
