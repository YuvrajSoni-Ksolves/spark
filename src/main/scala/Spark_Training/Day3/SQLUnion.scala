package Spark_Training.Day3

import Spark_Training.Constants._
import spark.implicits._

object SQLUnion {
  def main(args: Array[String]): Unit = {
    val simpleData = Seq(
      ("James", "Sales", "NY", 90000, 34, 10000),
      ("Michael", "Sales", "NY", 86000, 56, 20000),
      ("Robert", "Sales", "CA", 81000, 30, 23000),
      ("Maria", "Finance", "CA", 90000, 24, 23000)
    )
    val df = simpleData.toDF("employee_name", "department", "state", "salary", "age", "bonus")
    //    df.printSchema()

    //    root
    //    |-- employee_name: string (nullable = true)
    //    |-- department: string (nullable = true)
    //    |-- state: string (nullable = true)
    //    |-- salary: integer (nullable = false)
    //    |-- age: integer (nullable = false)
    //    |-- bonus: integer (nullable = false)
    //    df.show()
    //    +-------------+----------+-----+------+---+-----+
    //    |employee_name|department|state|salary|age|bonus|
    //    +-------------+----------+-----+------+---+-----+
    //    |        James|     Sales|   NY| 90000| 34|10000|
    //    |      Michael|     Sales|   NY| 86000| 56|20000|
    //    |       Robert|     Sales|   CA| 81000| 30|23000|
    //    |        Maria|   Finance|   CA| 90000| 24|23000|
    //    +-------------+----------+-----+------+---+-----+

    val simpleData2 = Seq(("James", "Sales", "NY", 90000, 34, 10000),
      ("Maria", "Finance", "CA", 90000, 24, 23000),
      ("Jen", "Finance", "NY", 79000, 53, 15000),
      ("Jeff", "Marketing", "CA", 80000, 25, 18000),
      ("Kumar", "Marketing", "NY", 91000, 50, 21000)
    )
    val df2 = simpleData2.toDF("employee_name", "department", "state", "salary", "age", "bonus")
    //    df2.show(false)
    //    +-------------+----------+-----+------+---+-----+
    //    |employee_name|department|state|salary|age|bonus|
    //    +-------------+----------+-----+------+---+-----+
    //    |James        |Sales     |NY   |90000 |34 |10000|
    //    |Maria        |Finance   |CA   |90000 |24 |23000|
    //    |Jen          |Finance   |NY   |79000 |53 |15000|
    //    |Jeff         |Marketing |CA   |80000 |25 |18000|
    //    |Kumar        |Marketing |NY   |91000 |50 |21000|
    //    +-------------+----------+-----+------+---+-----+

    val df3 = df.union(df2)
    //    df3.show(false)
    //    +-------------+----------+-----+------+---+-----+
    //    |employee_name|department|state|salary|age|bonus|
    //    +-------------+----------+-----+------+---+-----+
    //    |James        |Sales     |NY   |90000 |34 |10000|
    //      |Michael      |Sales     |NY   |86000 |56 |20000|
    //      |Robert       |Sales     |CA   |81000 |30 |23000|
    //      |Maria        |Finance   |CA   |90000 |24 |23000|
    //      |James        |Sales     |NY   |90000 |34 |10000|
    //      |Maria        |Finance   |CA   |90000 |24 |23000|
    //      |Jen          |Finance   |NY   |79000 |53 |15000|
    //      |Jeff         |Marketing |CA   |80000 |25 |18000|
    //      |Kumar        |Marketing |NY   |91000 |50 |21000|
    //      +-------------+----------+-----+------+---+-----+

    val df4 = df.union(df2).distinct()
    //    df4.show(false)
    //    +-------------+----------+-----+------+---+-----+
    //    |employee_name|department|state|salary|age|bonus|
    //    +-------------+----------+-----+------+---+-----+
    //    |James        |Sales     |NY   |90000 |34 |10000|
    //      |Maria        |Finance   |CA   |90000 |24 |23000|
    //      |Michael      |Sales     |NY   |86000 |56 |20000|
    //      |Robert       |Sales     |CA   |81000 |30 |23000|
    //      |Jeff         |Marketing |CA   |80000 |25 |18000|
    //      |Jen          |Finance   |NY   |79000 |53 |15000|
    //      |Kumar        |Marketing |NY   |91000 |50 |21000|
    //      +-------------+----------+-----+------+---+-----+
  }

}
