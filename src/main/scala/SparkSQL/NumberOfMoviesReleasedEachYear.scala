package com.example
package SparkSQL

object NumberOfMoviesReleasedEachYear extends App {
  val sparkSessionTest = new SparkSessionTest
  val spark = sparkSessionTest.spark

  import spark.implicits._

  val movies = spark.read.textFile("/home/knoldus/learning/learningSpark/hello-spark/src/main/resources/movies.dat")

  movies.createOrReplaceTempView("Movies")

  spark.sql(
    """select
      substring(split(value, '::')[1], 0, length(split(value,'::')[1])-7) as name,
      substring(split(value, '::')[1], length(split(value,'::')[1])-4, 4) as year
      from Movies
      """).createOrReplaceTempView("Movies_With_Year")

  spark.sql("select year ,count(name) as count from Movies_With_Year group by year order by count").show(false)
  spark.sql("select * from Movies_With_Year").show(false)




}
