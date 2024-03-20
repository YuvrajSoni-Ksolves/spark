package Spark_Training.Day3

import Spark_Training.Constants._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object SQLForEach {
  def main(args: Array[String]): Unit = {
    val structureData = Seq(
      Row("James", "", "Smith", "36636", "NewYork", 3100),
      Row("Michael", "Rose", "", "40288", "California", 4300),
      Row("Robert", "", "Williams", "42114", "Florida", 1400),
      Row("Maria", "Anne", "Jones", "39192", "Florida", 5500),
      Row("Jen", "Mary", "Brown", "34561", "NewYork", 3000)
    )

    val structureSchema = new StructType()
      .add("firstname", StringType)
      .add("middlename", StringType)
      .add("lastname", StringType)
      .add("id", StringType)
      .add("location", StringType)
      .add("salary", IntegerType)

    val df = spark.createDataFrame(sc.parallelize(structureData), structureSchema)
//    df.show(false)

    val longAcc = sc.longAccumulator("Sum Accumulator")
    df.foreach(f => {
      longAcc.add(f.getInt(5))
    })
    println("Accumulator value : " + longAcc.value)

    val longAcc2 = sc.longAccumulator("Sum Accumulator")
    val rdd = sc.parallelize(Seq(1,2,3,4,5,6))
    rdd.foreachPartition(partition =>{
      partition.foreach(fun=>{
        longAcc2.add(fun);
      })
    })
    println("Accumulator value : "+longAcc2.value)
  }

}
