package Spark_Training.Day3

import Spark_Training.Constants.spark
import spark.implicits._

object SQLGroupBy {
  def main(args: Array[String]): Unit = {
    val simpleData = Seq(("James", "Sales", "NY", 90000, 34, 10000),
      ("Michael", "Sales", "NY", 86000, 56, 20000),
      ("Robert", "Sales", "CA", 81000, 30, 23000),
      ("Maria", "Finance", "CA", 90000, 24, 23000),
      ("Raman", "Finance", "CA", 99000, 40, 24000),
      ("Scott", "Finance", "NY", 83000, 36, 19000),
      ("Jen", "Finance", "NY", 79000, 53, 15000),
      ("Jeff", "Marketing", "CA", 80000, 25, 18000),
      ("Kumar", "Marketing", "NY", 91000, 50, 21000)
    )
    val df = simpleData.toDF("employee_name", "department", "state", "salary", "age", "bonus")
    //    df.show()

    //    df.groupBy("department").sum("salary").show(false);

    //    df.groupBy("department").min("salary")

    //    df.groupBy("department").avg("salary").show(false)

    //    df.groupBy("department", "state")
    //      .sum("salary", "bonus")
    //      .show(false)

    import org.apache.spark.sql.functions._
    //    df.groupBy("department")
    //      .agg(
    //        sum("salary").as("sum_salary"),
    //        avg("salary").as("avg_salary"),
    //        sum("bonus").as("sum_bonus"),
    //        max("bonus").as("max_bonus")
    //      ).show(false)

    //    +----------+----------+-----------------+---------+---------+
    //    |department|sum_salary|avg_salary       |sum_bonus|max_bonus|
    //    +----------+----------+-----------------+---------+---------+
    //    |Sales     |257000    |85666.66666666667|53000    |23000    |
    //      |Finance   |351000    |87750.0          |81000    |24000    |
    //      |Marketing |171000    |85500.0          |39000    |21000    |
    //      +----------+----------+-----------------+---------+---------+

    df.groupBy("department")
      .agg(
        sum("salary").as("sum_salary"),
        avg("salary").as("avg_salary"),
        sum("bonus").as("sum_bonus"),
        max("bonus").as("max_bonus")
      ).where(col("sum_bonus") >= 50000)
      .show(false)

    //    +----------+----------+-----------------+---------+---------+
    //    |department|sum_salary|avg_salary       |sum_bonus|max_bonus|
    //    +----------+----------+-----------------+---------+---------+
    //    |Sales     |257000    |85666.66666666667|53000    |23000    |
    //      |Finance   |351000    |87750.0          |81000    |24000    |
    //      +----------+----------+-----------------+---------+---------+
  }

}
