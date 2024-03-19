package Spark_Training

import org.apache.spark.sql._

object Constants {

    val spark = SparkSession.builder()
      .appName("Spark Training")
      .master("local[1]")
      .getOrCreate()
    val sc = spark.sparkContext
}
