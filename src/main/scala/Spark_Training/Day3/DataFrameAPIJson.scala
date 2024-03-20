package Spark_Training.Day3

import Spark_Training.Constants._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types._

object DataFrameAPIJson {
  def main(args: Array[String]): Unit = {
    val df = spark.read.json("src/main/resources/zipcodes.json")
    //    df.printSchema()
    //    df.show(false)

    //    to read multiline json -> add option of multiline : true
    val multiline_df = spark.read.option("multiline", "true")
      .json("src/main/resources/multiline_zipcodes.json")
    //    multiline_df.show(false)

    //    reading multiple files
    val df2 = spark.read.option("multiline", "true").
      json("src/main/resources/zipcodes_streaming/zipcode1.json",
        "src/main/resources/zipcodes_streaming/zipcode2.json")
    //    df2.show(false)

    //reading all files in a directory
    val df3 = spark.read.option("multiline", "true")
      .json("src/main/resources/zipcodes.json")
    //        df3.show(false)

    //custom schema
    val schema = new StructType()
      .add("RecordNumber", IntegerType, false)
      .add("Zipcode", IntegerType, true)
      .add("ZipCodeType", StringType, true)
      .add("City", StringType, true)
      .add("State", StringType, true)
      .add("LocationType", StringType, true)
      .add("Lat", DoubleType, true)
      .add("Long", DoubleType, true)
      .add("Xaxis", IntegerType, true)
      .add("Yaxis", DoubleType, true)
      .add("Zaxis", DoubleType, true)
      .add("WorldRegion", StringType, true)
      .add("Country", StringType, true)
      .add("LocationText", StringType, true)
      .add("Location", StringType, true)
      .add("Decommisioned", BooleanType, true)
      .add("TaxReturnsFiled", StringType, true)
      .add("EstimatedPopulation", IntegerType, true)
      .add("TotalWages", IntegerType, true)
      .add("Notes", StringType, true)

    val df_with_schema = spark.read
      .schema(schema)
      .json("src/main/resources/zipcodes.json")

    //    df_with_schema.printSchema()
    //    df_with_schema.show(false)

    df2.write.
      json("src/main/resources/output_json/zipcodes_output")
  }
}
