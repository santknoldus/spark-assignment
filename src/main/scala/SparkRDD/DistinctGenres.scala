package com.example
package SparkRDD

object DistinctGenres extends App {
  val sparkSessionTest = new SparkSessionTest
  val spark = sparkSessionTest.spark

  val moviesRDD = spark.sparkContext.textFile("/home/knoldus/learning/learningSpark/hello-spark/src/main/resources/movies.dat")
  val genresRDD = moviesRDD.map(_.split("::")(2)).map(genres => genres.split('|')(0))
  val result = genresRDD.distinct()
  result.foreach(println)


}
