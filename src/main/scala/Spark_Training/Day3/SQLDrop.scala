package Spark_Training.Day3

import Spark_Training.Constants.spark
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object SQLDrop {
  def main(args: Array[String]): Unit = {
    val structureData = Seq(
      Row("James", "", "Smith", "36636", "NewYork", 3100),
      Row("Michael", "Rose", "", "40288", "California", 4300),
      Row("Robert", "", "Williams", "42114", "Florida", 1400),
      Row("Maria", "Anne", "Jones", "39192", "Florida", 5500),
      Row("Jen", "Mary", "Brown", "34561", "NewYork", 3000)
    )

    val structureSchema = new StructType()
      .add("firstname", StringType)
      .add("middlename", StringType)
      .add("lastname", StringType)
      .add("id", StringType)
      .add("location", StringType)
      .add("salary", IntegerType)

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(structureData), structureSchema)
    df.printSchema()

    //drop a column
    val df2 = df.drop("firstname")
    df2.printSchema()

    df.drop(df("firstname")).printSchema()

    df.drop(col("firstname")).printSchema()

    //drop multiple columns
    df.drop("firstname", "middlename", "lastname")
      .printSchema()

    val cols = Seq("firstname", "middlename", "lastname")
    df.drop(cols: _*)
      .printSchema()


  }
}
