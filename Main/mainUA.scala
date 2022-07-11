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
        "\n1 - Login/Switch Accounts" + //(admin,bigboss) (person2,word2)
        "\n2 - Select Column For a Certain Year" + //Neoplasms, 2021 //1800 rows
        "\n3 - Mortality By Age Group" + //Specified for 2020
        "\n4 - Average Monthly Deaths Per Year" + //We can see a spike in 2020
        "\n5 - Average Monthly Deaths Through The Years" + //December highest mortality
        "\n6 - Mortality By Month By Disease" + //Heart vs Chronic Respiratory                Over 15% USA
        "\n7 - Most Vulnerable Query December and Yearly" + // These are he people with highest mortality. Huge spike, Covid
        "\n8 - Least Vulnerable Query December and Yearly" + // 5-14 years old
        "\n9 Custom Query" + // Mostly to show tables
        "\nInsert" + //Any username and password
        "\nDelete" + //Any saved username
        "\nEnd ") //Ends program
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
        else if (confirmation == "Correct"){
          logginstatus = "User"
          adminstatus = "False"
        }
        backbot = scala.io.StdIn.readLine()
      }

      else if (running=="2" && logginstatus=="User") {
        functionsIMP.all2021column()
        backbot = scala.io.StdIn.readLine()
      }

      else if (running=="3" && logginstatus=="User") {
        functionsIMP.agegroup()
        backbot = scala.io.StdIn.readLine()
      }

      else if (running=="4" && logginstatus=="User") {
        functionsIMP.yeargroup()
        backbot = scala.io.StdIn.readLine()
      }

      else if (running=="5" && logginstatus=="User") {
        functionsIMP.monthgroup()
        backbot = scala.io.StdIn.readLine()
      }

      else if (running=="6" && logginstatus=="User") {
        functionsIMP.diseasegroup("Heart_Disease") //Least amount of people go to the hospital during holidays.
        functionsIMP.diseasegroup("Chronic_Respiratory_Diseases")
        backbot = scala.io.StdIn.readLine()
      }

      //functionsIMP.calcdiff()
      else if (running=="7" && logginstatus=="User") {
        functionsIMP.mostvulnerable()
        backbot = scala.io.StdIn.readLine()
      }

      else if (running=="8" && logginstatus=="User") {
        functionsIMP.leastvulnerable()
        backbot = scala.io.StdIn.readLine()
      }

      else if (running == "9" && adminstatus=="True"){
        functionsIMP.generalquery()
        backbot = scala.io.StdIn.readLine()
      }

      else if (running == "insert" && adminstatus=="True"){
        functionsIMP.insertvalue()
        backbot = scala.io.StdIn.readLine()
      }

      else if (running == "delete" && adminstatus=="True"){
        functionsIMP.modifyvalues()
        backbot = scala.io.StdIn.readLine()
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
