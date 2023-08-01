package com.example
package SparkSQL

object Create_Table extends App {

  val sparkSessionTest = new SparkSessionTest
  val spark = sparkSessionTest.spark

  spark.sql("drop database if exists SparkDatabase cascade")
  spark.sql("create database SparkDatabase")


  val moviesDF = spark.read.option("delimiter", "::").format("csv").load("/home/knoldus/learning/learningSpark/hello-spark/src/main/resources/movies.dat")
  moviesDF.toDF("moviesId","title","genres").createOrReplaceTempView("Movies")
  spark.sql("select * from Movies").write.mode("overwrite").saveAsTable("SparkDatabase.Movies")

  val ratingData = spark.read.option("delimiter", "::").format("csv").load("/home/knoldus/learning/learningSpark/hello-spark/src/main/resources/ratings.dat")
  ratingData.toDF("UserId","MovieId","Rating","TimeStamp").createOrReplaceTempView("Rating")
  spark.sql("select * from Rating").write.mode("overwrite").saveAsTable("SparkDatabase.Rating")

  val usersDataframe = spark.read.option("delimiter", "::").format("csv").load("/home/knoldus/learning/learningSpark/hello-spark/src/main/resources/users.dat")
  usersDataframe.toDF("UserID", "Gender", "Age", "Occupation", "Zip-Code").createOrReplaceTempView("Users")
  spark.sql("select * from Users").write.mode("overwrite").saveAsTable("SparkDatabase.Users")



}
