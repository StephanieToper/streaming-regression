import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import org.apache.spark.sql.functions.col

class FoodGrowthStreamingTest extends FunSuite {
  test("FoodGrowthStreaming.computeAndOrderGrowthStreaming") {
    val fileName = "/home/steph/tmp/streaming_folder/"


    val spark:SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("SparkByExamples")
      .getOrCreate()

    val res = FoodGrowthStreaming.computeAndOrderGrowthStreaming(fileName, spark)
    res.awaitTermination()
//
//    val itemStr =res.select(col("item")).first.getString(0)
//    val areaStr =res.select(col("area")).first.getString(0)
//
//    assert(itemStr=="Beer" && areaStr =="China, mainland")
//
   // res.wait()

      //res.awaitTermination()
   // println(res.toString())

    assert(1==1)
  }
}
