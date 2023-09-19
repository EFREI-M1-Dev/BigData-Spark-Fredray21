package org.example.SparkCounterTest

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.log4j.{Level, Logger}

object SparkCounterTest extends App {
  // Désactivez la journalisation INFO de Spark
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Configuration Spark
  val conf: SparkConf = new SparkConf().setAppName("name").setMaster("local")
    .set("spark.testing.memory", "2147480000")
  conf.set("spark.logConf", "false") // Pour afficher la configuration de journalisation

  val sc: SparkContext = new SparkContext(conf)

  try {
    val spark = SparkSession.builder()
      .appName("Counter")
      .config("spark.master", "local")
      .getOrCreate()

    // Chargez le fichier CSV en tant que DataFrame
    val csvFilePath = "C:\\dev\\EFREI\\spark-hello-world-example\\src\\cities.csv"
    val df: DataFrame = spark.read.option("header", "true").csv(csvFilePath)

    // Comptez le nombre de villes TOTAL dans la colonne 'label'
    val cityTotalCount = df.select("label").count()

    // Comptez le nombre de villes distinctes dans la colonne 'label'
    val cityCount = df.select("label").distinct().count()

    // Calculez la moyenne des latitudes et des longitudes
    val avgLatitude = df.selectExpr("avg(latitude)").collect()(0)(0).asInstanceOf[Double]
    val avgLongitude = df.selectExpr("avg(longitude)").collect()(0)(0).asInstanceOf[Double]

    // Groupement des données par le nom de la région
    // Register the DataFrame as a temporary table
    df.createOrReplaceTempView("city_data")

    // Use SQL expressions for aggregation with the correct column name
    val regionCityCount = spark.sql(
      """
  SELECT region_name AS name, COUNT(label) AS number
  FROM city_data
  GROUP BY region_name ORDER BY number DESC
""")

    // Display the DataFrame
    regionCityCount.show(regionCityCount.count.toInt, false)
    println("==============================DATA==============================")
    println(s"Nombre de villes total : $cityTotalCount")
    println(s"Nombre de villes distinctes : $cityCount")
    println(s"Moyenne des latitudes : $avgLatitude")
    println(s"Moyenne des longitudes : $avgLongitude")

    // Arrêtez la session Spark
    spark.stop()
  } catch {
    case e: Exception =>
      println(s"Une erreur s'est produite : ${e.getMessage}")
  }
}
