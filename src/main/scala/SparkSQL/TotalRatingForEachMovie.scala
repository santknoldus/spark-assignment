package com.example
package SparkSQL

object TotalRatingForEachMovie extends App {

  val sparkSessionTest = new SparkSessionTest
  val spark = sparkSessionTest.spark

  val ratings = spark.read.textFile("/home/knoldus/learning/learningSpark/hello-spark/src/main/resources/ratings.dat")
  ratings.createOrReplaceTempView("Ratings")

  spark.sql(
    """select
      split(value, '::')[1] as movieId,
      split(value, '::')[2] as rating
      from Ratings""").createOrReplaceTempView("New_Rating")

  spark.sql("select movieId, count(rating) as total_rating from New_Rating group by movieId order by cast(movieId as int) asc").show(false)

}
