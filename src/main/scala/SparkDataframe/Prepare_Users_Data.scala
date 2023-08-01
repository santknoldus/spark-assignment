package com.example
package SparkDataframe

object Prepare_Users_Data extends App {
  val sparkSessionTest = new SparkSessionTest
  val spark = sparkSessionTest.spark

  val usersDataframe = spark.read.option("delimiter", "::").format("csv").load("/home/knoldus/learning/learningSpark/hello-spark/src/main/resources/users.dat")
  usersDataframe.show(false)

  usersDataframe.toDF("UserID","Gender","Age","Occupation","Zip-Code").show(false)

}
