import org.scalatest.FunSuite
import org.apache.spark.sql.functions.{col}

class FoodGrowthTest extends FunSuite {
  test("FoodGrowth.computeAndOrderGrowth") {
    val fileName = "food_data.csv"

    val res = FoodGrowth.computeAndOrderGrowth(fileName)
    res.show()

    val itemStr =res.select(col("item")).first.getString(0)
    val areaStr =res.select(col("area")).first.getString(0)

    assert(itemStr=="Beer" && areaStr =="China, mainland")
  }
}
