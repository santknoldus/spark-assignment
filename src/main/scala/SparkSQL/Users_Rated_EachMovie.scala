package com.example
package SparkSQL

object Users_Rated_EachMovie extends App {

  val sparkSessionTest = new SparkSessionTest
  val spark = sparkSessionTest.spark

  val ratings = spark.read.textFile("/home/knoldus/learning/learningSpark/hello-spark/src/main/resources/ratings.dat")
  ratings.createOrReplaceTempView("Ratings")

  spark.sql(
    """select
       split(value, '::')[0] as userId,
       split(value, '::')[1] as movieId
       from Ratings""").createOrReplaceTempView("New_Rating")

  spark.sql("select movieId,count(userId) as total_users from New_Rating group by movieId").show(false)

}
