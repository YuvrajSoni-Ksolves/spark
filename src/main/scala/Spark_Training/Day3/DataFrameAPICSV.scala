package Spark_Training.Day3

import Spark_Training.Constants.spark
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, StringType, StructType}

object DataFrameAPICSV {
  def main(args: Array[String]): Unit = {
    //    val df = spark.read.csv("src/main/resources/zipcodes.csv")
    //    df.printSchema()
    //    df.show(false)

    val df2 = spark.read.format("csv")
      .option("header", true)
      .load("src/main/resources/zipcodes.csv")
    //    df2.show(false)

    val options = Map("inferSchema" -> "true",
      "delimeter" -> ",",
      "header" -> "true")
    val df3 = spark.read.
      options(options)
      .csv("src/main/resources/zipcodes.csv")
    //    df3.show(false)

    val schema = new StructType()
      .add("Column1", IntegerType, true)
      .add("Zipcode", IntegerType, true)
      .add("ZipCodeType", IntegerType, true)
      .add("City", StringType, true)
      .add("State", StringType, true)
      .add("LocationType", StringType, true)
      .add("Lat", DoubleType, true)
      .add("Long", DoubleType, true)
      .add("Xaxis", DoubleType, true)
      .add("Yaxis", DoubleType, true)
      .add("Zaxis", DoubleType, true)
      .add("WorldRegion", StringType, true)
      .add("Country", StringType, true)
      .add("LocaionText", StringType, true)
      .add("Location", StringType, true)
      .add("Decommisioned", BooleanType, true)
      .add("TaxReturnsFiled", StringType, true)
      .add("EstimatedPopulation", IntegerType, true)
      .add("TotalWages", IntegerType, true)
      .add("Notes", StringType, true)

    val df_with_schema = spark.read.
      option("header", "true")
      .schema(schema)
      .csv("src/main/resources/zipcodes.csv")
    //    df_with_schema.printSchema()
    //    df_with_schema.show()

    val df4 = spark.read.csv("src/main/resources/csv/text01.txt",
      "src/main/resources/csv/text02.txt",
      "src/main/resources/csv/text03.txt",
      "src/main/resources/csv/invalid.txt")
    //    df8.show(false)

    val df5 = spark.read.csv("src/main/resources/csv")
    //      df5.show()

    val df6 = spark.read.option("inferSchema", false)
      .option("header", "true")
      .options(Map("delimiter" -> ","))
      .csv("src/main/resources/zipcodes.csv")

    val df7 = df6.cache()
    //        df7.show()

    // Create a temporary view
    df7.createOrReplaceTempView("ZipCodes")

    // Query table
    spark.sql("select RecordNumber, Zipcode, City, State from ZipCodes")
      .show()

    df7.write.mode(SaveMode.Append)
      .csv("src/main/resources/zipcodesSaving")
  }
}
