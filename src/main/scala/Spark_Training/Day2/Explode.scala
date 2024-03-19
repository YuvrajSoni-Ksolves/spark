package Spark_Training.Day2

import Spark_Training.Constants._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{explode, explode_outer, posexplode, posexplode_outer}
import org.apache.spark.sql.types.StructType

object Explode {
  def main(args: Array[String]): Unit = {
    import spark.implicits._
    val arrayData = Seq(
      Row("James", List("Java", "Scala"), Map("hair" -> "black", "eye" -> "brown")),
      Row("Michal", List("Spark", "Java", null), Map("hair" -> "brown", "eye" -> null)),
      Row("Robert", List("CSharp", ""), Map("hair" -> "red", "eye" -> "")),
      Row("Washigton", null, null),
      Row("Jefferson", List(), Map())
    )
    val arraySchema = new StructType()
      .add("Name", StringType)
      .add("KnownLanguages", ArrayType(StringType))
      .add("Properties", MapType(StringType, StringType))
    val df = spark.createDataFrame(sc.parallelize(arrayData), arraySchema)

    df.select($"Name", explode($"knownLanguages")).show(false)

    df.select($"Name", explode($"properties")).show(false)

    df.select($"Name", explode_outer($"knownLanguages")).show(false)

    df.select($"Name", explode_outer($"properties")).show(false)

    df.select($"Name", posexplode($"knownLanguages")).show(false)

    df.select($"Name", posexplode($"properties")).show(false)

    df.select($"Name", posexplode_outer($"knownLanguages")).show(false)

    df.select($"Name", posexplode_outer($"properties")).show(false)
  }
}
