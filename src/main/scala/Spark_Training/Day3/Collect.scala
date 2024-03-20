package Spark_Training.Day3

import Spark_Training.Constants._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object Collect {
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
      .add("id", StringType)
      .add("gender", StringType)
      .add("salary", IntegerType)

    val df = spark.createDataFrame(sc.parallelize(data), schema)
    //    df.printSchema()

    //    root
    //    |-- name: struct (nullable = true)
    //    |    |-- firstname: string (nullable = true)
    //    |    |-- middlename: string (nullable = true)
    //    |    |-- lastname: string (nullable = true)
    //    |-- id: string (nullable = true)
    //    |-- gender: string (nullable = true)
    //    |-- salary: integer (nullable = true)

    //    df.show(false)
    //    +---------------------+-----+------+------+
    //    |name                 |id   |gender|salary|
    //    +---------------------+-----+------+------+
    //    |{James , , Smith}    |36636|M     |3000  |
    //      |{Michael , Rose, }   |40288|M     |4000  |
    //      |{Robert , , Williams}|42114|M     |4000  |
    //      |{Maria , Anne, Jones}|39192|F     |4000  |
    //      |{Jen, Mary, Brown}   |     |F     |-1    |
    //      +---------------------+-----+------+------+

    val colList = df.collectAsList()
    val colData = df.collect()
    colData.foreach(row => {
      val salary = row.getInt(3)
      //      println(salary)
    })
    //    3000
    //    4000
    //    4000
    //    4000
    //    -1
    colData.foreach(row => {
      val salary = row.getInt(3)
      val fullName: Row = row.getStruct(0)
      val firstName = fullName.getString(0)
      val middleName = fullName.get(1).toString
      val lastName = fullName.getAs[String]("lastname")
      //      println(s"$firstName $middleName $lastName $salary")
    })
    //    James   Smith 3000
    //    Michael  Rose  4000
    //    Robert   Williams 4000
    //    Maria  Anne Jones 4000
    //    Jen Mary Brown -1

    // select is better when selecting particular columns,
    //    but collect collects all the columns, which can cause space error
  }

}
