package Spark_Training.Day1

import org.apache.spark.sql._
import org.apache.spark.sql.types._

object DataSources {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().
      appName("Data Sources")
      .config("spark.master", "local")
      .getOrCreate()


    var carSchema = StructType(
      Array(
        StructField("Name", StringType),
        StructField("Miles_per_Gallon", IntegerType),
        StructField("Cylinders", IntegerType),
        StructField("Displacement", IntegerType),
        StructField("HorsePower", IntegerType),
        StructField("Weight_in_lbs", IntegerType),
        StructField("Acceleration", DoubleType),
        StructField("Year", StringType),
        StructField("Origin", StringType),
      )
    )

    /*
    * Reading a DF:
    - format
    * -schema or inferSchema = true
    *  zero or more options
    * */
    //    val filepath = "/src/main/"
    //    val carfDF = spark.read
    //      .format("json")
    //      .option("inferSchema", "true")
    //      .load(filepath)

    val filePath = "src/main/resources/zipcodes.json"
    //lazy evaluation
    //    val df = spark.read
    //      .format("json")
    //      .option("inferSchema", "true")
    //      .load(filePath)

    //    df.show()

    //alternative reading with options map
    val zipcodesDF = spark.read
      .format("json")
      .options(Map(
        "mode" -> "failFast",
        "path" -> filePath,
        "inferSchema" -> "true"
      )).load()
    //    zipcodesDF.show()

    /*Writing DFs
    *  -format
    * -save mode = overwrite, append, ignore, errorIfExists
    *  -path
    *  -zero or more options
    * */
    //    zipcodesDF.write
    //      .format("json")
    //      .mode(SaveMode.Overwrite)
    //      .option("path", "src/main/resources/zipcodes_dups.json")
    //      .save()
    //JSON FLAGS
    spark.read
      .option("dateFormat", "yyyy-MM-dd") // couple with schema : if spark fails parsing, it will put null
      .option("allowSingleQuotes", "true")
      .option("compression", "uncompressed") // bzip2, gzip, lz4, snappy, deflate
      .json(filePath)

    //CSV flags
    val zipcodesSchema = StructType(Array(
      StructField("Record", IntegerType),
      StructField("Zipcode", IntegerType),
      StructField("ZipCodeType", StringType),
      StructField("City", StringType),
      StructField("State", StringType),
      StructField("LocationType", StringType),
      StructField("Lat", DoubleType),
      StructField("Long", DoubleType),
      StructField("Xaxis", DoubleType),
      StructField("Yaxis", DoubleType),
      StructField("WorldRegion", StringType),
      StructField("Country", StringType),
      StructField("LocationText", StringType),
      StructField("Decommisioned", BooleanType),
      StructField("TaxReturnsField", IntegerType, nullable = true),
      StructField("EstimatedPopulation", IntegerType, nullable = true),
      StructField("Notes", StringType, nullable = true)
    ))

    val csvRead = spark.read
      .format("csv")
      .schema(zipcodesSchema)
      .option("header", "true")
      .option("sep", ",")
      .option("nullValue", "NULL")
      .load("src/main/resources/zipcodes.csv")

    //    csvRead.show()

    /*
        - parquet : Open Source compressed binary data storage format optimized for fast reading of columns,
        - parquet is default storage format for dataFrames
        - very predictable
        */
    //    zipcodesDF.write
    //      .mode(SaveMode.Overwrite)
    //      .save("src/main/resources/zipcodes_parquet")

    //text files
    spark.read
      .text("src/main/resources/test.txt")


    //reading from a remote database
    //   val employeesDF =  spark.read
    //      .format("jdbc")
    //      .option("driver", "org.postgresql.Driver")
    //      .option("url", "jdbc:postgresql://localhost:5432/rtjm")
    //      .option("user", "docker")
    //      .option("password", "docker")
    //      .option("dbtable", "public.employees")
    //      .load()


  }

}
