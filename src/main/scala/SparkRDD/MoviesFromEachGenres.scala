package com.example
package SparkRDD

object MoviesFromEachGenres extends App {
  val sparkSessionTest = new SparkSessionTest
  val spark = sparkSessionTest.spark

  val moviesRDD = spark.sparkContext.textFile("/home/knoldus/learning/learningSpark/hello-spark/src/main/resources/movies.dat")
  val splitMovie = moviesRDD.map(_.split("::")(2)).map(genres => (genres.split('|')(0),1))
  val result = splitMovie.reduceByKey(_+_)
  result.foreach(println)


}
