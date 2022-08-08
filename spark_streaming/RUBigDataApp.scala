package org.rubigdata

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.scalalang.typed
case class RuneData(material: String ,tpe: String, price: Int)
case class Weapon(name: String, num_hands: Int, speed: String, method: String)

object RUBigDataApp {
   def demo() = {
     val fnm = s"/opt/hadoop/rubigdata/rubigdata-test.txt"
     val spark = SparkSession.builder.appName("RUBigDataApp").getOrCreate()
     val data = spark.read.textFile(fnm).cache()
     val numAs = data.filter(line => line.contains("a")).count()
     val numEs = data.filter(line => line.contains("e")).count()
     println("\n########## OUTPUT ##########")
     println("Lines with a: %s, Lines with e: %s".format(numAs, numEs))
     println("########### END ############\n")
     spark.stop()
   }
   def simpleStreaming() = {
     val spark = SparkSession.builder.appName("RUBigDataApp").getOrCreate()
     spark.sparkContext.setLogLevel("WARN")
     import spark.implicits._

     val socketDF = spark.readStream
       .format("socket")
       .option("host", "localhost")
       .option("port", 9999)
       .load()

     val query = socketDF
       .writeStream
       .outputMode("append")
       .format("console")
       .option("truncate", false)
       .start()

     query.awaitTermination()
     spark.stop()
   }
   def a5Part1() = {
     val spark = SparkSession.builder.appName("RUBigDataApp").getOrCreate()
     import spark.implicits._
     val socketDF = spark.readStream
       .format("socket")
       .option("host", "localhost")
       .option("port", 9999)
       .load()

     val regex = "(^[A-Z].+) ([A-Z].+) was sold for (\\d+)gp$"

     val sales = socketDF
       .select(
         regexp_extract($"value", regex, 1) as "material",
         regexp_extract($"value", regex, 2) as "tpe",
         regexp_extract($"value", regex, 3).cast(IntegerType) as "price"
       )
       .as[RuneData]


     sales.createOrReplaceTempView("sales")
     val counts = spark.sql("SELECT material, SUM(price) AS total_sales " +
       "FROM sales " +
       "WHERE tpe = 'Dagger' " +
       "GROUP BY material")
     val mostExpensiveMaterial = spark.sql("SELECT material, AVG(price) as avg_price " +
       "FROM sales " +
       "GROUP BY material " +
       "ORDER BY avg_price DESC"
     )

     val mostExpensiveWeapon = spark.sql("SELECT tpe, AVG(price) as avg_price " +
       "FROM sales " +
       "GROUP BY tpe " +
       "ORDER BY avg_price DESC"
     )

     val item9850 = spark.sql("SELECT material, tpe, price, MIN(ABS(price - 9850)) AS price_dist " +
       "FROM sales " +
       "GROUP BY material, tpe, price " +
       "ORDER BY price_dist ASC"
     )

     val mithrilSword = spark.sql("SELECT material, AVG(price) " +
       "FROM sales " +
       "WHERE LOWER(tpe) LIKE '%sword%' " +
       "GROUP BY material"
     )

     val query = mithrilSword
       .writeStream
       .outputMode("complete")
       .format("console")
       .start()
     /*val query = sales
       .writeStream
       .outputMode("append")
       .format("console")
       .option("truncate", false)
       .start()*/

     query.awaitTermination()
     spark.stop()
   }
  def a5Part2() = {
    val spark = SparkSession.builder.appName("RUBigDataApp").getOrCreate()
    import spark.implicits._
    val socketDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    val regex = "(^[A-Z].+) ([A-Z].+) was sold for (\\d+)gp$"

    val sales = socketDF
      .select(
        regexp_extract($"value", regex, 1) as "material",
        regexp_extract($"value", regex, 2) as "tpe",
        regexp_extract($"value", regex, 3).cast(IntegerType) as "price"
      )
      .as[RuneData]

    val weapons = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("/opt/hadoop/rubigdata/weapons.csv")
      .as[Weapon]

    weapons.createOrReplaceTempView("weapons")
    sales.createOrReplaceTempView("sales")
    val combined = spark.sql("SELECT * FROM sales " +
      "INNER JOIN weapons " +
      "ON sales.tpe = weapons.name")

    combined.createOrReplaceTempView("combined")
    val expensiveWeaponTypes = spark.sql(
      "SELECT num_hands, AVG(price) " +
        "FROM combined " +
        "GROUP BY num_hands"
    )

    val mostGoldSpent = spark.sql(
      "SELECT num_hands, SUM(price) " +
        "FROM combined " +
        "GROUP BY num_hands"
    )

    val query = mostGoldSpent
      .writeStream
      .outputMode("complete")
      .format("console")
      .start()


    query.awaitTermination()
    spark.stop()

  }
  def main(args: Array[String]): Unit = {
     a5Part1()
   }
 }
