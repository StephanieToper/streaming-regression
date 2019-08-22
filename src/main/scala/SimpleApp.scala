/* SimpleApp.scala */
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SimpleApp {
  def main(args: Array[String]) {

    val configuration = new SparkConf().setAppName("SimpleApp").setMaster("local")
    val sc = new SparkContext(configuration)

    val logFile = "data.txt" // Should be some file on your system
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }
}