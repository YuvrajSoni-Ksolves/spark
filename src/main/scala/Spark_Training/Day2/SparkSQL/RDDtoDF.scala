package Spark_Training.Day2.SparkSQL

import Spark_Training.Constants._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object RDDtoDF {
  def main(args: Array[String]): Unit = {
    val columns = Seq("language", "users_count")
    val data = Seq(("Java", 2000), ("Python", 10000), ("Scala", 3000))

    import spark.implicits._
    val rdd = sc.parallelize(data)
    //    val dffFromRdd1 = rdd.toDF("langugage", "users_count");
    //    dffFromRdd1.printSchema()
    //    dffFromRdd1.show()

    //    val dffFromRdd2 = spark.createDataFrame(rdd).toDF(columns: _*)
    //    dffFromRdd2.show()

    val schema = StructType(Array(
      StructField("langugae", StringType, nullable = true),
      StructField("users", StringType, nullable = true)
    ))

    //    val rowRdd = rdd.map(attribute => Row(attribute._1, attribute._2))
    //    val dfFromRDD3 = spark.createDataFrame(rowRdd, schema)
    //    dfFromRDD3.show()
  }

}
