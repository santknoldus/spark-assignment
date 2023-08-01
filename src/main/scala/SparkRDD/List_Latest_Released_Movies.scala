package com.example
package SparkRDD

object List_Latest_Released_Movies extends App {

  val sparkSessionTest = new SparkSessionTest
  val spark = sparkSessionTest.spark

  val moviesRDD = spark.sparkContext.textFile("/home/knoldus/learning/learningSpark/hello-spark/src/main/resources/movies.dat")

  val moviesList = moviesRDD.map(movie => movie.split("::")(1))

  val years = moviesList.map(movie => movie.substring(movie.lastIndexOf("(") + 1, movie.lastIndexOf(")")))

  val latest = years.max

 val result = moviesList.filter(movie => movie.contains("("+latest+")"))
  result.foreach(println)

}
