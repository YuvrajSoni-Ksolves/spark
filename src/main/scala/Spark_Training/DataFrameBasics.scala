package Spark_Training

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object DataFrameBasics {
  def main(args: Array[String]): Unit = {
    //creating a spark session
    val spark = SparkSession.builder()
      .appName("Dataframes Basics")
      .config("spark.master", "local")
      .getOrCreate()

    //test spark is working fine
    //    println(spark)

    val filepath = "src/main/resources/zipcodes.json"
    val firstDF = spark.read
      .format("json")
      .option("ineferSchema", "true")
      .load(filepath)
    //similar to sql output
    //    firstDF.show(10)
    //    firstDF.printSchema()
    //    firstDF.take(10).foreach(println)  //get rows out of dataset

    val longType = LongType

    //schema
    val zipcodes = StructType(
      Array(
        StructField("City", StringType, true),
        StructField("Country", StringType, true)
      )
    )
    //obtain a schema
    val zipcodeDFSchema = firstDF.schema
    //    println(zipcodeDFSchema)

    val zipcodesDFSchema = spark.read
      .format("json")
      .schema(zipcodes)
      .load(filepath)

    zipcodesDFSchema.show()

    //create rows by hand
    val cars = Seq(
      ("porsche", 18, 8, 307, 130, 3504, 12.04, "1970-01-01", "USA"),
      ("bmw", 18, 8, 307, 130, 3504, 12.04, "1970-01-01", "USA"),
      ("audi", 18, 8, 307, 130, 3504, 12.04, "1970-01-01", "USA"),
      ("lamborghini", 18, 8, 307, 130, 3504, 12.04, "1970-01-01", "USA"),
      ("mercedes", 18, 8, 307, 130, 3504, 12.04, "1970-01-01", "IND" )
    )
    //note DFs have schemas, rows do not
    val manualCarsDF = spark.createDataFrame(cars) //schema)
    manualCarsDF.show()

    import spark.implicits._

    val manualCarsDFWithImplicits = cars.toDF("CarName", "Engine", "Gear", "Piston", "Brakes", "Doors", "Tyres", "Date", "Country")
    manualCarsDFWithImplicits.show()

    manualCarsDF.printSchema()
    manualCarsDFWithImplicits.printSchema()
  }
}
