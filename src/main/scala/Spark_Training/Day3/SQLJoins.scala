package Spark_Training.Day3

import Spark_Training.Constants.spark
import org.apache.spark.sql.functions.col

object SQLJoins {
  def main(args: Array[String]): Unit = {
    val emp = Seq((1, "Smith", -1, "2018", "10", "M", 3000),
      (2, "Rose", 1, "2010", "20", "M", 4000),
      (3, "Williams", 1, "2010", "10", "M", 1000),
      (4, "Jones", 2, "2005", "10", "F", 2000),
      (5, "Brown", 2, "2010", "40", "", -1),
      (6, "Brown", 2, "2010", "50", "", -1)
    )
    val empColumns = Seq("emp_id", "name", "superior_emp_id", "year_joined",
      "emp_dept_id", "gender", "salary")
    import spark.sqlContext.implicits._
    val empDF = emp.toDF(empColumns: _*)


    val dept = Seq(("Finance", 10),
      ("Marketing", 20),
      ("Sales", 30),
      ("IT", 40)
    )

    val deptColumns = Seq("dept_name", "dept_id")
    val deptDF = dept.toDF(deptColumns: _*)

    empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "inner")
    //      .show(false)
    //    +------+--------+---------------+-----------+-----------+------+------+---------+-------+
    //    |emp_id|name    |superior_emp_id|year_joined|emp_dept_id|gender|salary|dept_name|dept_id|
    //    +------+--------+---------------+-----------+-----------+------+------+---------+-------+
    //    |1     |Smith   |-1             |2018       |10         |M     |3000  |Finance  |10     |
    //      |2     |Rose    |1              |2010       |20         |M     |4000  |Marketing|20     |
    //      |3     |Williams|1              |2010       |10         |M     |1000  |Finance  |10     |
    //      |4     |Jones   |2              |2005       |10         |F     |2000  |Finance  |10     |
    //      |5     |Brown   |2              |2010       |40         |      |-1    |IT       |40     |
    //      +------+--------+---------------+-----------+-----------+------+------+---------+-------+

    empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "outer")
    //      .show(false)

    //    +------+--------+---------------+-----------+-----------+------+------+---------+-------+
    //    |emp_id|name    |superior_emp_id|year_joined|emp_dept_id|gender|salary|dept_name|dept_id|
    //    +------+--------+---------------+-----------+-----------+------+------+---------+-------+
    //    |1     |Smith   |-1             |2018       |10         |M     |3000  |Finance  |10     |
    //      |3     |Williams|1              |2010       |10         |M     |1000  |Finance  |10     |
    //      |4     |Jones   |2              |2005       |10         |F     |2000  |Finance  |10     |
    //      |2     |Rose    |1              |2010       |20         |M     |4000  |Marketing|20     |
    //      |null  |null    |null           |null       |null       |null  |null  |Sales    |30     |
    //      |5     |Brown   |2              |2010       |40         |      |-1    |IT       |40     |
    //      |6     |Brown   |2              |2010       |50         |      |-1    |null     |null   |
    //      +------+--------+---------------+-----------+-----------+------+------+---------+-------+

    empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "full")
    //      .show(false)
    //    +------+--------+---------------+-----------+-----------+------+------+---------+-------+
    //    |emp_id|name    |superior_emp_id|year_joined|emp_dept_id|gender|salary|dept_name|dept_id|
    //    +------+--------+---------------+-----------+-----------+------+------+---------+-------+
    //    |1     |Smith   |-1             |2018       |10         |M     |3000  |Finance  |10     |
    //      |3     |Williams|1              |2010       |10         |M     |1000  |Finance  |10     |
    //      |4     |Jones   |2              |2005       |10         |F     |2000  |Finance  |10     |
    //      |2     |Rose    |1              |2010       |20         |M     |4000  |Marketing|20     |
    //      |null  |null    |null           |null       |null       |null  |null  |Sales    |30     |
    //      |5     |Brown   |2              |2010       |40         |      |-1    |IT       |40     |
    //      |6     |Brown   |2              |2010       |50         |      |-1    |null     |null   |
    //      +------+--------+---------------+-----------+-----------+------+------+---------+-------+

    empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "full")
    //      .show(false)
    //    +------+--------+---------------+-----------+-----------+------+------+---------+-------+
    //    |emp_id|name    |superior_emp_id|year_joined|emp_dept_id|gender|salary|dept_name|dept_id|
    //    +------+--------+---------------+-----------+-----------+------+------+---------+-------+
    //    |1     |Smith   |-1             |2018       |10         |M     |3000  |Finance  |10     |
    //      |3     |Williams|1              |2010       |10         |M     |1000  |Finance  |10     |
    //      |4     |Jones   |2              |2005       |10         |F     |2000  |Finance  |10     |
    //      |2     |Rose    |1              |2010       |20         |M     |4000  |Marketing|20     |
    //      |null  |null    |null           |null       |null       |null  |null  |Sales    |30     |
    //      |5     |Brown   |2              |2010       |40         |      |-1    |IT       |40     |
    //      |6     |Brown   |2              |2010       |50         |      |-1    |null     |null   |
    //      +------+--------+---------------+-----------+-----------+------+------+---------+-------+

    empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "left")
    //      .show(false)
    //    +------+--------+---------------+-----------+-----------+------+------+---------+-------+
    //    |emp_id|name    |superior_emp_id|year_joined|emp_dept_id|gender|salary|dept_name|dept_id|
    //    +------+--------+---------------+-----------+-----------+------+------+---------+-------+
    //    |1     |Smith   |-1             |2018       |10         |M     |3000  |Finance  |10     |
    //      |2     |Rose    |1              |2010       |20         |M     |4000  |Marketing|20     |
    //      |3     |Williams|1              |2010       |10         |M     |1000  |Finance  |10     |
    //      |4     |Jones   |2              |2005       |10         |F     |2000  |Finance  |10     |
    //      |5     |Brown   |2              |2010       |40         |      |-1    |IT       |40     |
    //      |6     |Brown   |2              |2010       |50         |      |-1    |null     |null   |
    //      +------+--------+---------------+-----------+-----------+------+------+---------+-------+

    empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "leftouter")
    //      .show(false)
    //    +------+--------+---------------+-----------+-----------+------+------+---------+-------+
    //    |emp_id|name    |superior_emp_id|year_joined|emp_dept_id|gender|salary|dept_name|dept_id|
    //    +------+--------+---------------+-----------+-----------+------+------+---------+-------+
    //    |1     |Smith   |-1             |2018       |10         |M     |3000  |Finance  |10     |
    //      |2     |Rose    |1              |2010       |20         |M     |4000  |Marketing|20     |
    //      |3     |Williams|1              |2010       |10         |M     |1000  |Finance  |10     |
    //      |4     |Jones   |2              |2005       |10         |F     |2000  |Finance  |10     |
    //      |5     |Brown   |2              |2010       |40         |      |-1    |IT       |40     |
    //      |6     |Brown   |2              |2010       |50         |      |-1    |null     |null   |
    //      +------+--------+---------------+-----------+-----------+------+------+---------+-------+

    empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "leftsemi")
    //      .show(false)

    //    +------+--------+---------------+-----------+-----------+------+------+
    //    |emp_id|name    |superior_emp_id|year_joined|emp_dept_id|gender|salary|
    //    +------+--------+---------------+-----------+-----------+------+------+
    //    |1     |Smith   |-1             |2018       |10         |M     |3000  |
    //      |2     |Rose    |1              |2010       |20         |M     |4000  |
    //      |3     |Williams|1              |2010       |10         |M     |1000  |
    //      |4     |Jones   |2              |2005       |10         |F     |2000  |
    //      |5     |Brown   |2              |2010       |40         |      |-1    |
    //      +------+--------+---------------+-----------+-----------+------+------+

    empDF.as("emp1").join(empDF.as("emp2"),
        col("emp1.superior_emp_id") === col("emp2.emp_id"), "inner")
      .select(col("emp1.emp_id"), col("emp1.name"),
        col("emp2.name").as("superior_emp_id"),
        col("emp2.name").as("superior_emp_name"))
    //      .show(false)

    //    +------+--------+---------------+-----------------+
    //    |emp_id|name    |superior_emp_id|superior_emp_name|
    //    +------+--------+---------------+-----------------+
    //    |2     |Rose    |Smith          |Smith            |
    //      |3     |Williams|Smith          |Smith            |
    //      |4     |Jones   |Rose           |Rose             |
    //      |5     |Brown   |Rose           |Rose             |
    //      |6     |Brown   |Rose           |Rose             |
    //      +------+--------+---------------+-----------------+

    empDF.createOrReplaceTempView("EMP")
    deptDF.createOrReplaceTempView("DEPT")

    //SQL JOIN
    val joinDF = spark.sql("select * from EMP e, DEPT d where e.emp_dept_id == d.dept_id")

    //    joinDF.show(false)
    //    +------+--------+---------------+-----------+-----------+------+------+---------+-------+
    //    |emp_id|name    |superior_emp_id|year_joined|emp_dept_id|gender|salary|dept_name|dept_id|
    //    +------+--------+---------------+-----------+-----------+------+------+---------+-------+
    //    |1     |Smith   |-1             |2018       |10         |M     |3000  |Finance  |10     |
    //      |2     |Rose    |1              |2010       |20         |M     |4000  |Marketing|20     |
    //      |3     |Williams|1              |2010       |10         |M     |1000  |Finance  |10     |
    //      |4     |Jones   |2              |2005       |10         |F     |2000  |Finance  |10     |
    //      |5     |Brown   |2              |2010       |40         |      |-1    |IT       |40     |
    //      +------+--------+---------------+-----------+-----------+------+------+---------+-------+

    val joinDF2 = spark.sql("select * from EMP e INNER JOIN DEPT d on e.emp_dept_id == d.dept_id")
//    joinDF2.show(false)
//    +------+--------+---------------+-----------+-----------+------+------+---------+-------+
    //    |emp_id|name    |superior_emp_id|year_joined|emp_dept_id|gender|salary|dept_name|dept_id|
    //    +------+--------+---------------+-----------+-----------+------+------+---------+-------+
    //    |1     |Smith   |-1             |2018       |10         |M     |3000  |Finance  |10     |
    //      |2     |Rose    |1              |2010       |20         |M     |4000  |Marketing|20     |
    //      |3     |Williams|1              |2010       |10         |M     |1000  |Finance  |10     |
    //      |4     |Jones   |2              |2005       |10         |F     |2000  |Finance  |10     |
    //      |5     |Brown   |2              |2010       |40         |      |-1    |IT       |40     |
    //      +------+--------+---------------+-----------+-----------+------+------+---------+-------+

  }

}
