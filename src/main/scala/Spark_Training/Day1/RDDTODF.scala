package Spark_Training.Day1

import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

object RDDTODF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .getOrCreate()
    val sc = spark.sparkContext

    val rddSchema = StructType(Array(
      StructField("Name", StringType),
      StructField("Position", StringType),
      StructField("Age", IntegerType)
    ))

    val rdd = sc.parallelize(
      Seq(
        ("John", "Manager", 38),
        ("Mary", "Direcrtor", 45),
        ("Sally", "Engineer", 30)
      ))
    val rowRdd = rdd.map(r => Row(r._1, r._2, r._3))
    import spark.implicits._
    val rddWithSchema = spark.createDataFrame(rowRdd, rddSchema)
    rddWithSchema.show()
  }
}
