package Spark_Training.Day2

import Spark_Training.Constants.spark


object WindowFunctions {
  def main(args: Array[String]): Unit = {
    import spark.implicits._
    val simlpeData = Seq(
      ("James", "Sales", 3000),
      ("Michael", "Sales", 4600),
      ("Robert", "Sales", 4100),
      ("Maria", "Finance", 3000),
      ("James", "Sales", 3000),
      ("Scott", "Finance", 3300),
      ("Jen", "Finance", 3900),
      ("Jeff", "Marketing", 3000),
      ("Kumar", "Marketing", 2000),
      ("Saif", "Sales", 4100)

    )
    val df = simlpeData.toDF("emloyee_name", "department", "salary")
    //    df.show()
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.expressions.Window


    val windowSpec = Window.partitionBy("department").orderBy("Salary")
    /*
      - this window specification defines the partition criteria
      - and ordering criteria of the window
    */

    //    df.withColumn("row_number", row_number.over(windowSpec))
    //      .show()

    val totalamount = sum("salary").over(windowSpec)

    val salaryRank = rank().over(windowSpec)
    println("This is the resultant dataframe")
    val resultDF = df
      .withColumn("total_amount", totalamount)
      .withColumn("salary_rank", salaryRank)

    //    resultDF.show()

    //    df.withColumn("rank", rank().over(windowSpec))
    //      .show()
    //    df.withColumn("dense_rank", dense_rank().over(windowSpec))
    //      .show()

    //    df.withColumn("ntile", ntile(2).over(windowSpec))
    //      .show()

    //    df.withColumn("cume_dist", cume_dist().over(windowSpec))
    //      .show()

    //    df.withColumn("lag", lag("salary",2).over(windowSpec))
    //      .show()

    //    df.withColumn("lead", lead("salary",2).over(windowSpec))
    //      .show()

    val windowSpecAgg = Window.partitionBy("department")

    val aggDF = df.withColumn("row", row_number().over(windowSpec))
      .withColumn("avg", avg(col("salary")).over(windowSpecAgg))
      .withColumn("sum", sum(col("salary")).over(windowSpecAgg))
      .withColumn("min", min(col("salary")).over(windowSpecAgg))
      .withColumn("max", max(col("salary")).over(windowSpecAgg))
      .where(col("row") === 1).select("department", "avg", "sum", "min", "max")
      .show()
  }

}
