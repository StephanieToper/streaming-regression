import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType}
import setup.Food
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.catalyst.ScalaReflection

object FoodGrowthStreaming {

  def main(args: Array[String]): Unit = {
    val fileName = args(0)

    val spark:SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("SparkByExamples")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val res = FoodGrowthStreaming.computeAndOrderGrowthStreaming(fileName, spark)

    res.awaitTermination()
  }

  def computeAndOrderGrowthStreaming( folderPath : String, spark: SparkSession):StreamingQuery = {

    val userSchema = ScalaReflection.schemaFor[Food].dataType.asInstanceOf[StructType]

    val csvDF = spark
      .readStream
      .option("sep", ",").option("header", "true")
      .schema(userSchema)
      .csv(folderPath)


    val logUDF = udf( (x:Int) => math.log(x) )
    val dsLog = csvDF.select( col("quantity"), col("year"), col("area"), col("item"), logUDF(col("quantity")).as("log_quantity"), logUDF(col("year")).as("log_year")  )

    val gr= new Growth
    val res= dsLog.groupBy("item","area").agg(gr(col("log_quantity"), col("log_year")).as("growth")).orderBy(col("growth").desc)
    val t =res.writeStream.format("console").option("truncate","false").outputMode("complete").start()
    return t
  }
}
