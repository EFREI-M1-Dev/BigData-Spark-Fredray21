package org.example.SparkCounterTest
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.{Level, Logger}

import java.io.{FileOutputStream, PrintStream}

object SparkCounterTest extends App {
  // Désactivez la journalisation INFO de Spark
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Configuration Spark
  val conf: SparkConf = new SparkConf().setAppName("name").setMaster("local")
    .set("spark.testing.memory", "2147480000")
  conf.set("spark.logConf", "false") // Pour afficher la configuration de journalisation

  // Créer un contexte Spark
  new SparkContext(conf)

  try {
    // Créer une session Spark
    val spark = SparkSession.builder()
      .appName("Counter")
      .config("spark.master", "local")
      .getOrCreate()

    // Chargez le fichier CSV en tant que DataFrame
    val csvFilePath = "C:\\dev\\EFREI\\BigData-Spark-Fredray21\\src\\main\\scala\\cities.csv"
    val df: DataFrame = spark.read.option("header", "true").csv(csvFilePath)

    // Comptez le nombre de villes TOTAL dans la colonne 'label'
    val cityTotalCount = df.select("label").count()

    // Comptez le nombre de villes distinctes dans la colonne 'label'
    val cityCount = df.select("label").distinct().count()

    // Calculez la moyenne des latitudes et des longitudes
    val avgLatitude = df.selectExpr("avg(latitude)").collect()(0)(0).asInstanceOf[Double]
    val avgLongitude = df.selectExpr("avg(longitude)").collect()(0)(0).asInstanceOf[Double]

    // Groupement des données par le nom de la région
    df.createOrReplaceTempView("city_data")

    // Comptez le nombre de villes par région
    val regionCityCount = spark.sql(
      """
  SELECT region_name AS name, COUNT(label) AS number
  FROM city_data
  GROUP BY region_name ORDER BY number DESC
""")

    // Afficher les résultats dans la console
    regionCityCount.show(regionCityCount.count.toInt, truncate = false)
    println("==============================DATA==============================")
    println(s"Nombre de villes total : $cityTotalCount")
    println(s"Nombre de villes distinctes : $cityCount")
    println(s"Moyenne des latitudes : $avgLatitude")
    println(s"Moyenne des longitudes : $avgLongitude")


    // ############################## Affichage dans un fichier TXT ##############################

    // Créer un flux de sortie vers un fichier texte
    val outputFile = "./src/main/scala/output.txt"
    val fileOutputStream = new FileOutputStream(outputFile)
    val printStream = new PrintStream(fileOutputStream)

    // Rediriger la sortie standard vers le fichier
    System.setOut(printStream)

    // Afficher les résultats de la requête SQL dans le fichier txr
    Console.withOut(printStream) {
      regionCityCount.show(regionCityCount.count.toInt, truncate = false)
    }
    // Afficher les résultats dans le fichier txt
    printStream.println("==============================DATA==============================")
    printStream.println(s"Nombre de villes total : $cityTotalCount")
    printStream.println(s"Nombre de villes distinctes : $cityCount")
    printStream.println(s"Moyenne des latitudes : $avgLatitude")
    printStream.println(s"Moyenne des longitudes : $avgLongitude")

    // Fermer le flux de sortie après utilisation
    printStream.close()

    // Arrêtez la session Spark
    spark.stop()
  } catch {
    case e: Exception =>
      println(s"Une erreur s'est produite : ${e.getMessage}")
  }
}
