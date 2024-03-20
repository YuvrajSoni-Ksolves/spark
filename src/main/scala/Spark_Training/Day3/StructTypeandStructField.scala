package Spark_Training.Day3

import Spark_Training.Constants._
import org.apache.spark.sql._
import org.apache.spark.sql.types._


case class Name(first: String, last: String, middle: String)

case class Employee(fullName: Name, age: Integer, gender: String)

object StructTypeandStructField {
  def main(args: Array[String]): Unit = {
    val arrayStructureData = Seq(
      Row(Row("James ", "", "Smith"), List("Cricket", "Movies"), Map("hair" -> "black", "eye" -> "brown")),
      Row(Row("Michael ", "Rose", ""), List("Tennis"), Map("hair" -> "brown", "eye" -> "black")),
      Row(Row("Robert ", "", "Williams"), List("Cooking", "Football"), Map("hair" -> "red", "eye" -> "gray")),
      Row(Row("Maria ", "Anne", "Jones"), null, Map("hair" -> "blond", "eye" -> "red")),
      Row(Row("Jen", "Mary", "Brown"), List("Blogging"), Map("white" -> "black", "eye" -> "black"))
    )

    val arrayStructureSchema = new StructType()
      .add("name", new StructType()
        .add("firstname", StringType)
        .add("middlename", StringType)
        .add("lastname", StringType))
      .add("hobbies", ArrayType(StringType))
      .add("properties", MapType(StringType, StringType))

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(arrayStructureData), arrayStructureSchema)
    //    df.printSchema()
    //    root
    //    |-- name: struct (nullable = true)
    //    |    |-- firstname: string (nullable = true)
    //    |    |-- middlename: string (nullable = true)
    //    |    |-- lastname: string (nullable = true)
    //    |-- hobbies: array (nullable = true)
    //    |    |-- element: string (containsNull = true)
    //    |-- properties: map (nullable = true)
    //    |    |-- key: string
    //    |    |-- value: string (valueContainsNull = true)
    //    df.show()
    //    +--------------------+-------------------+--------------------+
    //    |                name|            hobbies|          properties|
    //    +--------------------+-------------------+--------------------+
    //    |   {James , , Smith}|  [Cricket, Movies]|{hair -> black, e...|
    //    |  {Michael , Rose, }|           [Tennis]|{hair -> brown, e...|
    //    |{Robert , , Willi...|[Cooking, Football]|{hair -> red, eye...|
    //    |{Maria , Anne, Jo...|               null|{hair -> blond, e...|
    //    |  {Jen, Mary, Brown}|         [Blogging]|{white -> black, ...|
    //    +--------------------+-------------------+--------------------+


    import org.apache.spark.sql.catalyst.ScalaReflection
    //case class to spark StructType

    val schema = ScalaReflection.schemaFor[Employee].dataType.asInstanceOf[StructType]

    val encoderSchema = Encoders.product[Employee].schema
    encoderSchema.printTreeString()
  }

}
