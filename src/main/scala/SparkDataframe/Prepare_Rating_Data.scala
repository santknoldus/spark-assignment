package com.example
package SparkDataframe

object Prepare_Rating_Data extends App {

  val sparkSessionTest = new SparkSessionTest
  val spark = sparkSessionTest.spark

  val ratingData = spark.read.option("delimiter", "::").format("csv").load("/home/knoldus/learning/learningSpark/hello-spark/src/main/resources/ratings.dat")
  ratingData.toDF("UserId","MovieId","Rating","TimeStamp").show(false)

}
