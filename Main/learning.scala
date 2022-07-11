import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object learning {
  def main(args: Array[String]): Unit = {

    testing.connectlink()

    testing.yearmin()






    testing.spark.close()
  }
}
