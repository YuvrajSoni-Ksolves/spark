package Spark_Training.Day2.SparkSQL

import Spark_Training.Constants.{sc, spark}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{array_contains, flatten}
import org.apache.spark.sql.types._

object DFfromListAndSeq {
  def main(args: Array[String]): Unit = {
    //    val df2 = spark.read.csv("src/main/resources/zipcodes20.csv")
    //    df2.show()

    //    val df2 = spark.read.json("src/main/resources/zipcodes.json")
    //    df2.show()

    val arrayStructureData = Seq(
      Row(Row("James", "", "Smith"), List("Java", "Scala", "C++"), "OH", "M"),
      Row(Row("Anna", "Rose", ""), List("Spark", "Java", "C++"), "NY", "F"),
      Row(Row("Julia", "", "Williams"), List("CSharp", "VB"), "OH", "F"),
      Row(Row("Maria", "Anne", "Jones"), List("CSharp", "VB"), "NY", "M"),
      Row(Row("Jen", "Mary", "Brown"), List("CSharp", "VB"), "NY", "M"),
      Row(Row("Mike", "Mary", "Williams"), List("Python", "VB"), "OH", "M")
    )

    val arrayStructureSchema = new StructType()
      .add("name", new StructType()
        .add("firstname", StringType)
        .add("middlename", StringType)
        .add("lastname", StringType))
      .add("languages", ArrayType(StringType))
      .add("state", StringType)
      .add("gender", StringType)

    val df = spark.createDataFrame(sc.parallelize((arrayStructureData)), arrayStructureSchema)
    df.printSchema()
    df.show()

    df.where(df("state") === "OH")
      .show(false)

    df.where("gender == 'M'")
      .show(false)

    df.where(df("state") === "OH" && df("gender") === "M")
      .show(false)

    df.where("state == 'OH' AND  gender == 'M'")
      .show(false)

    df.where(array_contains(df("languages"), "Java"))
      .show(false)

    df.where(df("name.middlename").equalTo("Mary"))
      .show(false)
  }

}
