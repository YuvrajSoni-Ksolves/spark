package Spark_Training.Day3

import Spark_Training.Constants.spark

object SQLDistinct {
  def main(args: Array[String]): Unit = {
    import spark.implicits._
    val simpleData = Seq(
      ("James", "Sales", 3000),
      ("Michael", "Sales", 4600),
      ("Robert", "Sales", 4100),
      ("Maria", "Finance", 3000),
      ("James", "Sales", 3000),
      ("Scott", "Finance", 3300),
      ("Jen", "Finance", 3900),
      ("Jeff", "Marketing", 3000),
      ("Kumar", "Marketing", 2000),
      ("Saif", "Sales", 4100),
      ("James", "Sales", 3000)
    )
    val df = simpleData.toDF("employee_name", "department", "salary")
    //    df.show()

    val distinctDF = df.distinct()
    println("Distinct count : " + distinctDF.count())
    //    distinctDF.show(false)

    //removing duplicates
    val df2 = df.dropDuplicates()
    println("Distinct count : " + df2.count())
    //    df2.show(false)

    val dropDisDF = df.dropDuplicates("employee_name")
    println("Distinct count of department & salary : " + dropDisDF.count())
    dropDisDF.show(false)

  }

}
