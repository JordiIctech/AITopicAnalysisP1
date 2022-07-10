import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object mainUA {
  def main(args: Array[String]): Unit = {

    functionsIMP.connectlink()

    println("What column do you wish to see?")
    functionsIMP.all2021column(scala.io.StdIn.readLine()) // First one has to ask prompt after connecting.

    println("What is your username?")
    functionsIMP.passwordmanagement()

    functionsIMP.spark.close()
  }
}
