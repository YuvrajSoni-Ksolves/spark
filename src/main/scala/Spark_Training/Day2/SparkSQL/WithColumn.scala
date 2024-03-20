package Spark_Training.Day2.SparkSQL

import Spark_Training.Constants._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types._

object WithColumn {
  def main(args: Array[String]): Unit = {
    val data = Seq(Row(Row("James;", "", "Smith"), "36636", "M", "3000"),
      Row(Row("Michael", "Rose", ""), "40288", "M", "4000"),
      Row(Row("Robert", "", "Williams"), "42114", "M", "4000"),
      Row(Row("Maria", "Anne", "Jones"), "39192", "F", "4000"),
      Row(Row("Jen", "Mary", "Brown"), "", "F", "-1")
    )

    val schema = new StructType()
      .add("name", new StructType()
        .add("firstname", StringType)
        .add("middlename", StringType)
        .add("lastname", StringType))
      .add("dob", StringType)
      .add("gender", StringType)
      .add("salary", StringType)

    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    df.withColumn("Country", lit("USA"))
      .withColumn("anotherColumn", lit("anotherValue"))
      .show(false)

    df.withColumn("salary", col("salary") * 100)
      .show(false)

    df.withColumn("CopiedColumn", col("salary") * 10)
      .show(false)

    df.withColumn("salary", col("salary").cast("Integer"))
      .show(false)

    df.createOrReplaceTempView("Person")
    spark.sql("SELECT salary*100 as salary, salary * -1 as CopiedColumn, 'USA' as country FROM PERSON" ).show(false)

  }

}
