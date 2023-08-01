package com.example
package SparkRDD

object Movies_Starting_With_Letters extends App {
  val sparkSessionTest = new SparkSessionTest
  val spark = sparkSessionTest.spark

  val moviesRDD = spark.sparkContext.textFile("/home/knoldus/learning/learningSpark/hello-spark/src/main/resources/movies.dat")

  val firstChar = moviesRDD.map(movie => movie.split("::")(1)(0))
  val result =
    firstChar
    .filter(char => char.isLetter || char.isDigit)
    .map(chars => (chars, 1))
    .reduceByKey(_+_)
  result.foreach(println)



}
