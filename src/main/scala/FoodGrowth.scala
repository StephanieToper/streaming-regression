import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import setup.Food
import org.apache.spark.sql.functions.udf

object FoodGrowth {

  def main(args: Array[String]): Unit = {
    val fileName = "food_data.csv"
    val res = FoodGrowth.computeAndOrderGrowth(fileName)
    res.show()
  }

  def computeAndOrderGrowth( fileName : String): DataFrame = {

    val configuration = new SparkConf ().setAppName ("FoodGrowth").setMaster ("local")
    val sc = new SparkContext (configuration)

    val spark = SparkSession.builder.appName ("Simple Application").getOrCreate ()
    import spark.implicits._

    val ds = spark.read.format ("csv")
    .option ("header", "true")
    .option ("inferSchema", "true")
    .load (fileName)
    .as[Food]

    val gr = new Growth

    val logUDF = udf ((x: Int) => math.log (x) )

    val dsLog = ds.select ($"quantity", $"year", $"area", $"item", logUDF ($"quantity").as ("log_quantity"), logUDF ($"year").as ("log_year") )

    val dsGrowthLog = dsLog.groupBy ("area", "item").agg (gr (dsLog.col ("log_quantity"), dsLog.col ("log_year") ).as ("growth") )
    dsGrowthLog.orderBy ($"growth".desc)
  }
}
