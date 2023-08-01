package com.example
package SparkSQL

import org.apache.spark.sql.functions.{col, split}

object Oldest_Movies extends App {

  val sparkSessionTest = new SparkSessionTest
  val spark = sparkSessionTest.spark

  import spark.implicits._

  val movies_rdd = spark.sparkContext.textFile("/home/knoldus/learning/learningSpark/hello-spark/src/main/resources/movies.dat")
  movies_rdd.toDF.createOrReplaceTempView("movies_view")

  spark.sql(
    """ select
  split(value,'::')[0] as movieid,
  split(value,'::')[1] as moviename,
  substring(split(value,'::')[1],length(split(value,'::')[1])-4,4) as year
  from movies_view """).createOrReplaceTempView("movies");
  spark.sql("select * from movies").show()
  var result = spark.sql("Select * from movies m1 where m1.year=(Select min(m2.year) from movies m2)").repartition(1).rdd.saveAsTextFile("result")


}
