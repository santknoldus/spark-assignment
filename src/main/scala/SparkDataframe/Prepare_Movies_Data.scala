package com.example
package SparkDataframe

object Prepare_Movies_Data extends App {

  val sparkSessionTest = new SparkSessionTest
  val spark = sparkSessionTest.spark

  val moviesDF = spark.read.option("delimiter", "::").format("csv").load("/home/knoldus/learning/learningSpark/hello-spark/src/main/resources/movies.dat")

  moviesDF.toDF("MovieID", "title", "Genres").show()


}
