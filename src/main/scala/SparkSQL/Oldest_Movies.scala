package com.example
package SparkSQL

import org.apache.spark.sql.functions.{col, split}

object Oldest_Movies extends App {

  val sparkSessionTest = new SparkSessionTest
  val spark = sparkSessionTest.spark

  spark.sql("create database MyDb")

  val moviesDf = spark.read.option("delimiter", "::").format("csv").load("/home/knoldus/learning/learningSpark/hello-spark/src/main/resources/movies.dat")



}
