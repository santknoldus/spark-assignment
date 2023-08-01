package com.example
package SparkSQL

object MoviesForEachRating extends App {

  val sparkSessionTest = new SparkSessionTest
  val spark = sparkSessionTest.spark

  import spark.implicits._

  val rating = spark.read.textFile("/home/knoldus/learning/learningSpark/hello-spark/src/main/resources/ratings.dat")
  rating.createOrReplaceTempView("Rating")

  spark.sql(
    """select
      split(value, '::')[2] as ratings
      from Rating""").createOrReplaceTempView("OnlyRating")

  spark.sql("select ratings as Rating ,count(ratings) as Number_of_movies from OnlyRating group by ratings").show(false)

  //spark.sql("select * from OnlyRating").show()

}
