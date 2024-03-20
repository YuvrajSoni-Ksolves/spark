package Spark_Training.Day3

import Spark_Training.Constants._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

class SparkMapandMapPartitions extends Serializable {
  def combine(fname: String, mname: String, lname: String): String = {
    fname + "," + mname + "," + lname
  }
}

object SparkMapandMapPartitions {
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

    val df2 = spark.createDataFrame(
      sc.parallelize(structureData), structureSchema)

    df2.printSchema()
    //    root
    //    |-- firstname: string (nullable = true)
    //    |-- middlename: string (nullable = true)
    //    |-- lastname: string (nullable = true)
    //    |-- id: string (nullable = true)
    //    |-- location: string (nullable = true)
    //    |-- salary: integer (nullable = true)

    df2.show(false)
    //    +---------+----------+--------+-----+----------+------+
    //    |firstname|middlename|lastname|id   |location  |salary|
    //    +---------+----------+--------+-----+----------+------+
    //    |James    |          |Smith   |36636|NewYork   |3100  |
    //    |Michael  |Rose      |        |40288|California|4300  |
    //    |Robert   |          |Williams|42114|Florida   |1400  |
    //    |Maria    |Anne      |Jones   |39192|Florida   |5500  |
    //    |Jen      |Mary      |Brown   |34561|NewYork   |3000  |
    //    +---------+----------+--------+-----+----------+------+

    import spark.implicits._
    val df3 = df2.map(row => {
      val util = new SparkMapandMapPartitions()
      val fullName = util.combine(row.getString(0), row.getString(1), row.getString(2))
      (fullName, row.getString(3), row.getInt(5))
    })

    val df3Map = df3.toDF("fullName", "id", "salary")
    df3Map.printSchema()
    //    root
    //    |-- fullName: string (nullable = true)
    //    |-- id: string (nullable = true)
    //    |-- salary: integer (nullable = false)


    df3Map.show(false)
    //    +----------------+-----+------+
    //    |fullName        |id   |salary|
    //    +----------------+-----+------+
    //    |James,,Smith    |36636|3100  |
    //    |Michael,Rose,   |40288|4300  |
    //    |Robert,,Williams|42114|1400  |
    //    |Maria,Anne,Jones|39192|5500  |
    //    |Jen,Mary,Brown  |34561|3000  |
    //    +----------------+-----+------+

    // Map Partitions
    val df4 = df2.mapPartitions(iterator => {
      //Do the heavy lifting
      // Like database connections etc
      val util = new SparkMapandMapPartitions()
      val res = iterator.map(row => {
        val fullName = util.combine(row.getString(0), row.getString(1), row.getString(2))
        (fullName, row.getString(3), row.getInt(5))
      })
      res
    })
    val df4part = df4.toDF("fullName", "id", "salary")
    df4part.printSchema()
    //
    //    root
    //    |-- fullName: string (nullable = true)
    //    |-- id: string (nullable = true)
    //    |-- salary: integer (nullable = false)
    df4part.show(false)
    //    +----------------+-----+------+
    //    |fullName        |id   |salary|
    //    +----------------+-----+------+
    //    |James,,Smith    |36636|3100  |
    //      |Michael,Rose,   |40288|4300  |
    //      |Robert,,Williams|42114|1400  |
    //      |Maria,Anne,Jones|39192|5500  |
    //      |Jen,Mary,Brown  |34561|3000  |
    //      +----------------+-----+------+
  }

}
