package com.example
package SparkRDD

object TOP_10_Movies extends App {

  val sparkSessionTest = new SparkSessionTest
  val spark = sparkSessionTest.spark

  val ratingRDD = spark.sparkContext.textFile("/home/knoldus/learning/learningSpark/hello-spark/src/main/resources/ratings.dat")

  val splitData = ratingRDD.map(_.split("::")(1).trim)
  val movies_pair = splitData.map(movieID => (movieID, 1))
  val reducedMovie  = movies_pair.reduceByKey(_+_)
  val sortedMovie = reducedMovie.sortBy(_._2,ascending = false)
  //sortedMovie.foreach(println)

 val top_10_movies = spark.sparkContext.parallelize(sortedMovie.take(10))


  val moviesRDD = spark.sparkContext.textFile("/home/knoldus/learning/learningSpark/hello-spark/src/main/resources/movies.dat")
  val splitMovie = moviesRDD.map(movieInfo => (movieInfo.split("::")(0).trim,movieInfo.split("::")(1).trim))
  val outer_join = splitMovie.join(top_10_movies)
  val result = outer_join.sortBy(_._2._2, ascending = false).map(movie => movie._1 + "," + movie._2._1 + "," + movie._2._2)

  //outer_join.foreach(println)
  result.foreach(println)


  // top_10_movies.foreach(println)


}
