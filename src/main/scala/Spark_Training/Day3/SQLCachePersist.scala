package Spark_Training.Day3

import Spark_Training.Constants._
import org.apache.spark.sql.functions.col

object SQLCachePersist {
  def main(args: Array[String]): Unit = {
    val df = spark.read.options(Map("inferSchema" -> "true",
      "delimeter" -> ",",
      "header" -> "true")
    ).csv("src/main/resources/zipcodes.csv")

    val df2 = df.where(col("State") === "PR").cache()
    df.show(false)
    /*
    * +------------+-------+-----------+-------------------+-----+--------------+-----+-------+-----+-----+-----+-----------+-------+-----------------------+----------------------------+-------------+---------------+-------------------+----------+-------------+
|RecordNumber|Zipcode|ZipCodeType|City               |State|LocationType  |Lat  |Long   |Xaxis|Yaxis|Zaxis|WorldRegion|Country|LocationText           |Location                    |Decommisioned|TaxReturnsFiled|EstimatedPopulation|TotalWages|Notes        |
+------------+-------+-----------+-------------------+-----+--------------+-----+-------+-----+-----+-----+-----------+-------+-----------------------+----------------------------+-------------+---------------+-------------------+----------+-------------+
|1           |704    |STANDARD   |PARC PARQUE        |PR   |NOT ACCEPTABLE|17.96|-66.22 |0.38 |-0.87|0.3  |NA         |US     |Parc Parque, PR        |NA-US-PR-PARC PARQUE        |false        |null           |null               |null      |null         |
|2           |704    |STANDARD   |PASEO COSTA DEL SUR|PR   |NOT ACCEPTABLE|17.96|-66.22 |0.38 |-0.87|0.3  |NA         |US     |Paseo Costa Del Sur, PR|NA-US-PR-PASEO COSTA DEL SUR|false        |null           |null               |null      |null         |
|10          |709    |STANDARD   |BDA SAN LUIS       |PR   |NOT ACCEPTABLE|18.14|-66.26 |0.38 |-0.86|0.31 |NA         |US     |Bda San Luis, PR       |NA-US-PR-BDA SAN LUIS       |false        |null           |null               |null      |null         |
|61391       |76166  |UNIQUE     |CINGULAR WIRELESS  |TX   |NOT ACCEPTABLE|32.72|-97.31 |-0.1 |-0.83|0.54 |NA         |US     |Cingular Wireless, TX  |NA-US-TX-CINGULAR WIRELESS  |false        |null           |null               |null      |null         |
|61392       |76177  |STANDARD   |FORT WORTH         |TX   |PRIMARY       |32.75|-97.33 |-0.1 |-0.83|0.54 |NA         |US     |Fort Worth, TX         |NA-US-TX-FORT WORTH         |false        |2126           |4053               |122396986 |null         |
|61393       |76177  |STANDARD   |FT WORTH           |TX   |ACCEPTABLE    |32.75|-97.33 |-0.1 |-0.83|0.54 |NA         |US     |Ft Worth, TX           |NA-US-TX-FT WORTH           |false        |2126           |4053               |122396986 |null         |
|4           |704    |STANDARD   |URB EUGENE RICE    |PR   |NOT ACCEPTABLE|17.96|-66.22 |0.38 |-0.87|0.3  |NA         |US     |Urb Eugene Rice, PR    |NA-US-PR-URB EUGENE RICE    |false        |null           |null               |null      |null         |
|39827       |85209  |STANDARD   |MESA               |AZ   |PRIMARY       |33.37|-111.64|-0.3 |-0.77|0.55 |NA         |US     |Mesa, AZ               |NA-US-AZ-MESA               |false        |14962          |26883              |563792730 |no NWS data, |
|39828       |85210  |STANDARD   |MESA               |AZ   |PRIMARY       |33.38|-111.84|-0.31|-0.77|0.55 |NA         |US     |Mesa, AZ               |NA-US-AZ-MESA               |false        |14374          |25446              |471000465 |null         |
|49345       |32046  |STANDARD   |HILLIARD           |FL   |PRIMARY       |30.69|-81.92 |0.12 |-0.85|0.51 |NA         |US     |Hilliard, FL           |NA-US-FL-HILLIARD           |false        |3922           |7443               |133112149 |null         |
|49346       |34445  |PO BOX     |HOLDER             |FL   |PRIMARY       |28.96|-82.41 |0.11 |-0.86|0.48 |NA         |US     |Holder, FL             |NA-US-FL-HOLDER             |false        |null           |null               |null      |null         |
|49347       |32564  |STANDARD   |HOLT               |FL   |PRIMARY       |30.72|-86.67 |0.04 |-0.85|0.51 |NA         |US     |Holt, FL               |NA-US-FL-HOLT               |false        |1207           |2190               |36395913  |null         |
|49348       |34487  |PO BOX     |HOMOSASSA          |FL   |PRIMARY       |28.78|-82.61 |0.11 |-0.86|0.48 |NA         |US     |Homosassa, FL          |NA-US-FL-HOMOSASSA          |false        |null           |null               |null      |null         |
|10          |708    |STANDARD   |BDA SAN LUIS       |PR   |NOT ACCEPTABLE|18.14|-66.26 |0.38 |-0.86|0.31 |NA         |US     |Bda San Luis, PR       |NA-US-PR-BDA SAN LUIS       |false        |null           |null               |null      |null         |
|3           |704    |STANDARD   |SECT LANAUSSE      |PR   |NOT ACCEPTABLE|17.96|-66.22 |0.38 |-0.87|0.3  |NA         |US     |Sect Lanausse, PR      |NA-US-PR-SECT LANAUSSE      |false        |null           |null               |null      |null         |
|54354       |36275  |PO BOX     |SPRING GARDEN      |AL   |PRIMARY       |33.97|-85.55 |0.06 |-0.82|0.55 |NA         |US     |Spring Garden, AL      |NA-US-AL-SPRING GARDEN      |false        |null           |null               |null      |null         |
|54355       |35146  |STANDARD   |SPRINGVILLE        |AL   |PRIMARY       |33.77|-86.47 |0.05 |-0.82|0.55 |NA         |US     |Springville, AL        |NA-US-AL-SPRINGVILLE        |false        |4046           |7845               |172127599 |null         |
|54356       |35585  |STANDARD   |SPRUCE PINE        |AL   |PRIMARY       |34.37|-87.69 |0.03 |-0.82|0.56 |NA         |US     |Spruce Pine, AL        |NA-US-AL-SPRUCE PINE        |false        |610            |1209               |18525517  |null         |
|76511       |27007  |STANDARD   |ASH HILL           |NC   |NOT ACCEPTABLE|36.4 |-80.56 |0.13 |-0.79|0.59 |NA         |US     |Ash Hill, NC           |NA-US-NC-ASH HILL           |false        |842            |1666               |28876493  |null         |
|76512       |27203  |STANDARD   |ASHEBORO           |NC   |PRIMARY       |35.71|-79.81 |0.14 |-0.79|0.58 |NA         |US     |Asheboro, NC           |NA-US-NC-ASHEBORO           |false        |8355           |15228              |215474318 |null         |
+------------+-------+-----------+-------------------+-----+--------------+-----+-------+-----+-----+-----+-----------+-------+-----------------------+----------------------------+-------------+---------------+-------------------+----------+-------------+
only showing top 20 rows*/
    val a = System.currentTimeMillis()

    df.show(false)
    //    +------------+-------+-----------+-------------------+-----+--------------+-----+-------+-----+-----+-----+-----------+-------+-----------------------+----------------------------+-------------+---------------+-------------------+----------+-------------+
    //    |RecordNumber|Zipcode|ZipCodeType|City               |State|LocationType  |Lat  |Long   |Xaxis|Yaxis|Zaxis|WorldRegion|Country|LocationText           |Location                    |Decommisioned|TaxReturnsFiled|EstimatedPopulation|TotalWages|Notes        |
    //    +------------+-------+-----------+-------------------+-----+--------------+-----+-------+-----+-----+-----+-----------+-------+-----------------------+----------------------------+-------------+---------------+-------------------+----------+-------------+
    //    |1           |704    |STANDARD   |PARC PARQUE        |PR   |NOT ACCEPTABLE|17.96|-66.22 |0.38 |-0.87|0.3  |NA         |US     |Parc Parque, PR        |NA-US-PR-PARC PARQUE        |false        |null           |null               |null      |null         |
    //      |2           |704    |STANDARD   |PASEO COSTA DEL SUR|PR   |NOT ACCEPTABLE|17.96|-66.22 |0.38 |-0.87|0.3  |NA         |US     |Paseo Costa Del Sur, PR|NA-US-PR-PASEO COSTA DEL SUR|false        |null           |null               |null      |null         |
    //      |10          |709    |STANDARD   |BDA SAN LUIS       |PR   |NOT ACCEPTABLE|18.14|-66.26 |0.38 |-0.86|0.31 |NA         |US     |Bda San Luis, PR       |NA-US-PR-BDA SAN LUIS       |false        |null           |null               |null      |null         |
    //      |61391       |76166  |UNIQUE     |CINGULAR WIRELESS  |TX   |NOT ACCEPTABLE|32.72|-97.31 |-0.1 |-0.83|0.54 |NA         |US     |Cingular Wireless, TX  |NA-US-TX-CINGULAR WIRELESS  |false        |null           |null               |null      |null         |
    //      |61392       |76177  |STANDARD   |FORT WORTH         |TX   |PRIMARY       |32.75|-97.33 |-0.1 |-0.83|0.54 |NA         |US     |Fort Worth, TX         |NA-US-TX-FORT WORTH         |false        |2126           |4053               |122396986 |null         |
    //      |61393       |76177  |STANDARD   |FT WORTH           |TX   |ACCEPTABLE    |32.75|-97.33 |-0.1 |-0.83|0.54 |NA         |US     |Ft Worth, TX           |NA-US-TX-FT WORTH           |false        |2126           |4053               |122396986 |null         |
    //      |4           |704    |STANDARD   |URB EUGENE RICE    |PR   |NOT ACCEPTABLE|17.96|-66.22 |0.38 |-0.87|0.3  |NA         |US     |Urb Eugene Rice, PR    |NA-US-PR-URB EUGENE RICE    |false        |null           |null               |null      |null         |
    //      |39827       |85209  |STANDARD   |MESA               |AZ   |PRIMARY       |33.37|-111.64|-0.3 |-0.77|0.55 |NA         |US     |Mesa, AZ               |NA-US-AZ-MESA               |false        |14962          |26883              |563792730 |no NWS data, |
    //    |39828       |85210  |STANDARD   |MESA               |AZ   |PRIMARY       |33.38|-111.84|-0.31|-0.77|0.55 |NA         |US     |Mesa, AZ               |NA-US-AZ-MESA               |false        |14374          |25446              |471000465 |null         |
    //      |49345       |32046  |STANDARD   |HILLIARD           |FL   |PRIMARY       |30.69|-81.92 |0.12 |-0.85|0.51 |NA         |US     |Hilliard, FL           |NA-US-FL-HILLIARD           |false        |3922           |7443               |133112149 |null         |
    //      |49346       |34445  |PO BOX     |HOLDER             |FL   |PRIMARY       |28.96|-82.41 |0.11 |-0.86|0.48 |NA         |US     |Holder, FL             |NA-US-FL-HOLDER             |false        |null           |null               |null      |null         |
    //      |49347       |32564  |STANDARD   |HOLT               |FL   |PRIMARY       |30.72|-86.67 |0.04 |-0.85|0.51 |NA         |US     |Holt, FL               |NA-US-FL-HOLT               |false        |1207           |2190               |36395913  |null         |
    //      |49348       |34487  |PO BOX     |HOMOSASSA          |FL   |PRIMARY       |28.78|-82.61 |0.11 |-0.86|0.48 |NA         |US     |Homosassa, FL          |NA-US-FL-HOMOSASSA          |false        |null           |null               |null      |null         |
    //      |10          |708    |STANDARD   |BDA SAN LUIS       |PR   |NOT ACCEPTABLE|18.14|-66.26 |0.38 |-0.86|0.31 |NA         |US     |Bda San Luis, PR       |NA-US-PR-BDA SAN LUIS       |false        |null           |null               |null      |null         |
    //      |3           |704    |STANDARD   |SECT LANAUSSE      |PR   |NOT ACCEPTABLE|17.96|-66.22 |0.38 |-0.87|0.3  |NA         |US     |Sect Lanausse, PR      |NA-US-PR-SECT LANAUSSE      |false        |null           |null               |null      |null         |
    //      |54354       |36275  |PO BOX     |SPRING GARDEN      |AL   |PRIMARY       |33.97|-85.55 |0.06 |-0.82|0.55 |NA         |US     |Spring Garden, AL      |NA-US-AL-SPRING GARDEN      |false        |null           |null               |null      |null         |
    //      |54355       |35146  |STANDARD   |SPRINGVILLE        |AL   |PRIMARY       |33.77|-86.47 |0.05 |-0.82|0.55 |NA         |US     |Springville, AL        |NA-US-AL-SPRINGVILLE        |false        |4046           |7845               |172127599 |null         |
    //      |54356       |35585  |STANDARD   |SPRUCE PINE        |AL   |PRIMARY       |34.37|-87.69 |0.03 |-0.82|0.56 |NA         |US     |Spruce Pine, AL        |NA-US-AL-SPRUCE PINE        |false        |610            |1209               |18525517  |null         |
    //      |76511       |27007  |STANDARD   |ASH HILL           |NC   |NOT ACCEPTABLE|36.4 |-80.56 |0.13 |-0.79|0.59 |NA         |US     |Ash Hill, NC           |NA-US-NC-ASH HILL           |false        |842            |1666               |28876493  |null         |
    //      |76512       |27203  |STANDARD   |ASHEBORO           |NC   |PRIMARY       |35.71|-79.81 |0.14 |-0.79|0.58 |NA         |US     |Asheboro, NC           |NA-US-NC-ASHEBORO           |false        |8355           |15228              |215474318 |null         |
    //      +------------+-------+-----------+-------------------+-----+--------------+-----+-------+-----+-----+-----+-----------+-------+-----------------------+----------------------------+-------------+---------------+-------------------+----------+-------------+
    //    only showing top 20 rows
    val b = System.currentTimeMillis()
    println(b - a)
    // 149
    val df3 = df2.where(col("Zipcode") === 704)
    println(df2.count())
    df3.show(false)
    //    +------------+-------+-----------+-------------------+-----+--------------+-----+-------+-----+-----+-----+-----------+-------+-----------------------+----------------------------+-------------+---------------+-------------------+----------+-------------+
    //    |RecordNumber|Zipcode|ZipCodeType|City               |State|LocationType  |Lat  |Long   |Xaxis|Yaxis|Zaxis|WorldRegion|Country|LocationText           |Location                    |Decommisioned|TaxReturnsFiled|EstimatedPopulation|TotalWages|Notes        |
    //    +------------+-------+-----------+-------------------+-----+--------------+-----+-------+-----+-----+-----+-----------+-------+-----------------------+----------------------------+-------------+---------------+-------------------+----------+-------------+
    //    |1           |704    |STANDARD   |PARC PARQUE        |PR   |NOT ACCEPTABLE|17.96|-66.22 |0.38 |-0.87|0.3  |NA         |US     |Parc Parque, PR        |NA-US-PR-PARC PARQUE        |false        |null           |null               |null      |null         |
    //      |2           |704    |STANDARD   |PASEO COSTA DEL SUR|PR   |NOT ACCEPTABLE|17.96|-66.22 |0.38 |-0.87|0.3  |NA         |US     |Paseo Costa Del Sur, PR|NA-US-PR-PASEO COSTA DEL SUR|false        |null           |null               |null      |null         |
    //      |10          |709    |STANDARD   |BDA SAN LUIS       |PR   |NOT ACCEPTABLE|18.14|-66.26 |0.38 |-0.86|0.31 |NA         |US     |Bda San Luis, PR       |NA-US-PR-BDA SAN LUIS       |false        |null           |null               |null      |null         |
    //      |61391       |76166  |UNIQUE     |CINGULAR WIRELESS  |TX   |NOT ACCEPTABLE|32.72|-97.31 |-0.1 |-0.83|0.54 |NA         |US     |Cingular Wireless, TX  |NA-US-TX-CINGULAR WIRELESS  |false        |null           |null               |null      |null         |
    //      |61392       |76177  |STANDARD   |FORT WORTH         |TX   |PRIMARY       |32.75|-97.33 |-0.1 |-0.83|0.54 |NA         |US     |Fort Worth, TX         |NA-US-TX-FORT WORTH         |false        |2126           |4053               |122396986 |null         |
    //      |61393       |76177  |STANDARD   |FT WORTH           |TX   |ACCEPTABLE    |32.75|-97.33 |-0.1 |-0.83|0.54 |NA         |US     |Ft Worth, TX           |NA-US-TX-FT WORTH           |false        |2126           |4053               |122396986 |null         |
    //      |4           |704    |STANDARD   |URB EUGENE RICE    |PR   |NOT ACCEPTABLE|17.96|-66.22 |0.38 |-0.87|0.3  |NA         |US     |Urb Eugene Rice, PR    |NA-US-PR-URB EUGENE RICE    |false        |null           |null               |null      |null         |
    //      |39827       |85209  |STANDARD   |MESA               |AZ   |PRIMARY       |33.37|-111.64|-0.3 |-0.77|0.55 |NA         |US     |Mesa, AZ               |NA-US-AZ-MESA               |false        |14962          |26883              |563792730 |no NWS data, |
    //    |39828       |85210  |STANDARD   |MESA               |AZ   |PRIMARY       |33.38|-111.84|-0.31|-0.77|0.55 |NA         |US     |Mesa, AZ               |NA-US-AZ-MESA               |false        |14374          |25446              |471000465 |null         |
    //      |49345       |32046  |STANDARD   |HILLIARD           |FL   |PRIMARY       |30.69|-81.92 |0.12 |-0.85|0.51 |NA         |US     |Hilliard, FL           |NA-US-FL-HILLIARD           |false        |3922           |7443               |133112149 |null         |
    //      |49346       |34445  |PO BOX     |HOLDER             |FL   |PRIMARY       |28.96|-82.41 |0.11 |-0.86|0.48 |NA         |US     |Holder, FL             |NA-US-FL-HOLDER             |false        |null           |null               |null      |null         |
    //      |49347       |32564  |STANDARD   |HOLT               |FL   |PRIMARY       |30.72|-86.67 |0.04 |-0.85|0.51 |NA         |US     |Holt, FL               |NA-US-FL-HOLT               |false        |1207           |2190               |36395913  |null         |
    //      |49348       |34487  |PO BOX     |HOMOSASSA          |FL   |PRIMARY       |28.78|-82.61 |0.11 |-0.86|0.48 |NA         |US     |Homosassa, FL          |NA-US-FL-HOMOSASSA          |false        |null           |null               |null      |null         |
    //      |10          |708    |STANDARD   |BDA SAN LUIS       |PR   |NOT ACCEPTABLE|18.14|-66.26 |0.38 |-0.86|0.31 |NA         |US     |Bda San Luis, PR       |NA-US-PR-BDA SAN LUIS       |false        |null           |null               |null      |null         |
    //      |3           |704    |STANDARD   |SECT LANAUSSE      |PR   |NOT ACCEPTABLE|17.96|-66.22 |0.38 |-0.87|0.3  |NA         |US     |Sect Lanausse, PR      |NA-US-PR-SECT LANAUSSE      |false        |null           |null               |null      |null         |
    //      |54354       |36275  |PO BOX     |SPRING GARDEN      |AL   |PRIMARY       |33.97|-85.55 |0.06 |-0.82|0.55 |NA         |US     |Spring Garden, AL      |NA-US-AL-SPRING GARDEN      |false        |null           |null               |null      |null         |
    //      |54355       |35146  |STANDARD   |SPRINGVILLE        |AL   |PRIMARY       |33.77|-86.47 |0.05 |-0.82|0.55 |NA         |US     |Springville, AL        |NA-US-AL-SPRINGVILLE        |false        |4046           |7845               |172127599 |null         |
    //      |54356       |35585  |STANDARD   |SPRUCE PINE        |AL   |PRIMARY       |34.37|-87.69 |0.03 |-0.82|0.56 |NA         |US     |Spruce Pine, AL        |NA-US-AL-SPRUCE PINE        |false        |610            |1209               |18525517  |null         |
    //      |76511       |27007  |STANDARD   |ASH HILL           |NC   |NOT ACCEPTABLE|36.4 |-80.56 |0.13 |-0.79|0.59 |NA         |US     |Ash Hill, NC           |NA-US-NC-ASH HILL           |false        |842            |1666               |28876493  |null         |
    //      |76512       |27203  |STANDARD   |ASHEBORO           |NC   |PRIMARY       |35.71|-79.81 |0.14 |-0.79|0.58 |NA         |US     |Asheboro, NC           |NA-US-NC-ASHEBORO           |false        |8355           |15228              |215474318 |null         |
    //      +------------+-------+-----------+-------------------+-----+--------------+-----+-------+-----+-----+-----+-----------+-------+-----------------------+----------------------------+-------------+---------------+-------------------+----------+-------------+
    //    only showing top 20 rows
  }

}