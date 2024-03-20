package Spark_Training.Day3

import Spark_Training.Constants._
import org.apache.spark.sql.functions.{col, udf}

object UDF {
  def main(args: Array[String]): Unit = {

    import spark.implicits._
    val columns = Seq("Seqno", "Quote")
    val data = Seq(
      (1, "This is first line"),
      (2, "This is second line"),
      (3, "This is third line")
    )
    val df = data.toDF(columns: _*)
    //    df.show(false)
    /*
    +-----+-------------------+
    |Seqno|Quote              |
    +-----+-------------------+
    |1    |This is firstline  |
      |2    |This is second line|
    |3    |This is third line |
    +-----+-------------------+
    */

    val convertCase = (strQuote: String) => {
      val arr = strQuote.split(" ")
      arr.map(f => {
        f.substring(0, 1).toUpperCase + f.substring(1, f.length)
      }).mkString(" ")
    }
    val contUDF = udf(convertCase)
    df.select(col("Seqno"),
      contUDF(col("Quote")).as("Quote")).show(false)

    //    +-----+-------------------+
    //    |Seqno|Quote              |
    //    +-----+-------------------+
    //    |1    |This Is First Line |
    //    |2    |This Is Second Line|
    //    |3    |This Is Third Line |
    //    +-----+-------------------+

    /*
    * No null check is applied -> create udfs very carefully
    * udfs are blackboxes : we will loose all the optimization done by spark*/

  }

}
