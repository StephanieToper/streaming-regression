import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession


object test {
  def main(args: Array[String]) {

    val configuration = new SparkConf().setAppName("FoodGrowth").setMaster("local")
    val sc = new SparkContext(configuration)

    // Bazic groupByKey example in scala
    val x = sc.parallelize(Array(("USA", 1), ("USA", 2), ("India", 1), ("UK", 1), ("India", 4), ("India", 9),("USA", 8), ("USA", 3), ("India", 4), ("UK", 6), ("UK", 9), ("UK", 5)), 3)
    // x: org.apache.spark.rdd.RDD[(String, Int)] =
    //  ParallelCollectionRDD[0] at parallelize at <console>:24

    // groupByKey with default partitions
    val y = x.groupByKey

    y.foreach(println)
    //--------------------------------------------------------------------------------------------

    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()

    //read csv file transform to pair rdd
    val foodDataset = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("food_data.csv")

    foodDataset.take(10).foreach(println)

    val foodRDD = foodDataset.rdd
    println("-------RDD---------------")
    foodRDD.take(10).foreach(println)



    //Create a key pair value RDD so you can regroup the keys and apply the UDF/Growth to each pair key value
    val test1 = foodRDD.map(x => ((x(1).toString()) +"_" +x(3).toString(),(x(10),x(11)))) //maybe concatenate the key
    test1.take(10).foreach(println)
    val test2 = test1.groupByKey() //why is this throwing an error
    println("-------test2---------------")
    test2.take(10).foreach(println)


  }

}
