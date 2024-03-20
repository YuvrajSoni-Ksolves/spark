package Spark_Training.Day3

import Spark_Training.Constants._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._

object SQLWithColumnRenamed {
  def main(args: Array[String]): Unit = {
    val data = Seq(Row(Row("James ", "", "Smith"), "36636", "M", 3000),
      Row(Row("Michael ", "Rose", ""), "40288", "M", 4000),
      Row(Row("Robert ", "", "Williams"), "42114", "M", 4000),
      Row(Row("Maria ", "Anne", "Jones"), "39192", "F", 4000),
      Row(Row("Jen", "Mary", "Brown"), "", "F", -1)
    )
    val schema = new StructType()
      .add("name", new StructType()
        .add("firstname", StringType)
        .add("middlename", StringType)
        .add("lastname", StringType))
      .add("dob", StringType)
      .add("gender", StringType)
      .add("salary", IntegerType)


    val df = spark.createDataFrame(sc.parallelize(data), schema)
    //    df.printSchema()

    //    df.withColumnRenamed("dob", "DateOfBirth")
    //      .withColumnRenamed("salary", "income")
    //      .show(false)
    val schema2 = new StructType()
      .add("fname", StringType)
      .add("middlename", StringType)
      .add("lname", StringType)

    df.select(col("name").cast(schema2),
        col("dob"),
        col("gender"),
        col("salary"))
      .printSchema()

    df.select(col("name.firstname").as("fname"),
        col("name.middlename").as("mname"),
        col("name.lastname").as("lname"),
        col("dob"), col("gender"), col("salary"))
      .printSchema()

    val df4 = df.withColumn("fname", col("name.firstname"))
      .withColumn("mname", col("name.middlename"))
      .withColumn("lname", col("name.lastname"))
      .drop("name")

    df4.printSchema()

    val old_columns = Seq("dob", "gender", "salary", "fname", "mname", "lname")
    val new_columns = Seq("date of birth", "sex", "income", "firstname", "middlename", "lastname")
    val columnList = old_columns.zip(new_columns).map(f => {
      col(f._1).as(f._2)
    })
    val df5 = df4.select(columnList: _*)
    df5.printSchema()

  }

}
