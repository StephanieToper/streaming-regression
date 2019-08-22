import java.time.Duration

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import setup.Food
import org.apache.spark.sql.Column
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.streaming.dstream.DStream

import scala.concurrent.duration.Duration
//import org.apache.spark.sql.types.{DataType, DateType, TimestampType}
import org.apache.spark.sql.functions.{col, udf}

object FoodGrowthStreaming {

  def main(args: Array[String]): Unit = {
    val fileName = args(0)//"/home/steph/tmp/streaming_folder/"

    val spark:SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("SparkByExamples")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val res = FoodGrowthStreaming.computeAndOrderGrowthStreaming(fileName, spark)

    res.awaitTermination()
  }

  def computeAndOrderGrowthStreaming( folderPath : String, spark: SparkSession):StreamingQuery = {


    import org.apache.spark.sql.catalyst.ScalaReflection
    val userSchema = ScalaReflection.schemaFor[Food].dataType.asInstanceOf[StructType]

//    val userSchema = new StructType()
//      .add("area_abbreviation", "string")
//      .add("area_code", "integer")
//      .add("area", "string")
//      .add("item_code", "integer")
//      .add("item", "string")
//      .add("element_code", "integer")
//      .add("element", "string")
//      .add("unit", "string")
//      .add("latitude","double")
//      .add("longitude","double")
//      .add("year","integer")
//      .add("quantity","integer")


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
