package Spark_Training.Day2

import Spark_Training.Constants.spark
import org.apache.spark.sql.functions.expr

object PivotandUnpivot {
  def main(args: Array[String]): Unit = {
    val data = Seq(("Banana", 1000, "USA"), ("Carrots", 1500, "USA"), ("Beans", 1600, "USA"),
      ("Orange", 2000, "USA"), ("Orange", 2000, "USA"), ("Banana", 400, "China"),
      ("Carrots", 1200, "China"), ("Beans", 1500, "China"), ("Orange", 4000, "China"),
      ("Banana", 2000, "Canada"), ("Carrots", 2000, "Canada"), ("Beans", 2000, "Mexico"))

    import spark.sqlContext.implicits._
    val df = data.toDF("Product", "Amount", "Country")
    //    df.show()

    //pivot
    //    val pivotDF = df.groupBy("Product").pivot("Country").sum("Amount")
    //    pivotDF.show()
    //    val countries = Seq("USA", "China", "Canada", "Mexico")
    //    val pivotDF = df.groupBy("Product").pivot("Country", countries).sum("Amount")
    //    pivotDF.show()

    //using two phase aggregation
    println("Code Example")
    val pivotDF = df.groupBy("Product", "Country")
      .sum("Amount")
      .groupBy("Product")
      .pivot("Country")
      .sum("sum(Amount)")
    //    pivotDF.show()

    //unpivot
    val unpivotDF = pivotDF.select($"Product",
        expr("stack(3,'Canada', Canada,'China', China, 'Mexico' , Mexico) as (Country, Total)"))
      .where("Total is not null")
    unpivotDF.show()

  }

}
