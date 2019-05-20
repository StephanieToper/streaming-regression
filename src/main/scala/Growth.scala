import com.sun.org.apache.xalan.internal.xsltc.compiler.util.IntType
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.{StructType, StructField, DoubleType, LongType, DataType, ArrayType}


case class RegressionData(slope: Double)

class Regression  {

  import org.apache.commons.math3.stat.regression.SimpleRegression

  def roundAt(p: Int)(n: Double): Double = { val s = math pow (10, p); (math round n * s) / s }

  def getRegression(data: List[Long]): RegressionData = {
    val regression: SimpleRegression  = new SimpleRegression()
    data.view.zipWithIndex.foreach { d =>
      regression.addData(d._2.toDouble, d._1.toDouble)
    }

    RegressionData(roundAt(3)(regression.getSlope()))
  }
}


class Growth extends UserDefinedAggregateFunction {
  import java.util.ArrayList

  // This is the input fields for your aggregate function.
  override def inputSchema: org.apache.spark.sql.types.StructType =

   new StructType()
     .add("year", LongType)
     .add("quantity", LongType)

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType =
    new StructType().add("buff", ArrayType(LongType))


  // This is the output type of your aggregatation function.
  override def dataType: DataType =
    new StructType()
      .add("slope", DoubleType)

  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0, new ArrayList[Long]())
  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val longList: ArrayList[Long]  = new ArrayList[Long](buffer.getList(0))
    longList.add(input.getLong(0));
    buffer.update(0, longList);
  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val longList: ArrayList[Long] = new ArrayList[Long](buffer1.getList(0))
    longList.addAll(buffer2.getList(0))
    buffer1.update(0, longList)
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    import scala.collection.JavaConverters._
    val list = buffer.getList(0).asScala.toList
    val regression = new Regression
    regression.getRegression(list)
  }
}
