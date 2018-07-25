package wxl.com.person

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.elasticsearch.spark.rdd.EsSpark
import org.elasticsearch.spark.sql.EsSparkSQL
import wxl.com.person.SearchEnum.SearchEnum

/**
  * Created by wangxl6 on 2018/7/25.
  */
object Person {
  def apply(s: String, s1: String, toInt: Int) = ???


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test")
      .config("spark.es.nodes", "10.80.139.180")
      .config("spark.es.port", "9201")
      .config("spark.es.nodes.discovery", "ture")
      .config("spark.es.nodes.client.only", "false")
      .config("spark.es.nodes.wan.only", "false")
      .config("spark.es.net.http.auth.user", "elastic")
      .config("spark.es.net.http.auth.pass", "changeme")

      //指定返回字段
//        .config("spark.es.read.field.include","姓")
      /*
      # include
        es.read.field.include = *name, address.*
      # exclude
        es.read.field.exclude = *.created
      */
      .master("local[1]").getOrCreate()

//    write(spark)

    search(spark,SearchEnum.DF)

  }
  def search(spark : SparkSession,searchType : SearchEnum): Unit ={
    searchType match {
      case SearchEnum.LOAD =>
        println("LOAD ……")
        val sparkDF1 = spark.read.format("org.elasticsearch.spark.sql").load("people/person")
        sparkDF1.select("姓", "年龄").collect().foreach(println(_))
      case SearchEnum.CREATE_TB =>
        println("CREATE_TB ……")
        spark.sql("CREATE TEMPORARY TABLE myPeople USING org.elasticsearch.spark.sql OPTIONS  ( resource 'people/person')")
        spark.sql("select * from myPeople").show()

      case SearchEnum.DF =>
        println("DF ……")
        val people =  EsSparkSQL.esDF(spark,"people/person","?q=wang")
//        println(people.schema.treeString)
        people.printSchema()
        people.show()
//        people.toDF().distinct()
        EsSparkSQL.saveToEs(people.toDF().distinct(),"people1/person")

      case _ =>
        println("do nothing ……")
    }
  }
  def write(spark : SparkSession): Unit ={
    var dataFrame = spark.read.csv("C:\\Users\\wangxl6\\Desktop\\people.txt").toDF("姓","名","年龄")
    dataFrame.show()
    dataFrame.rdd.foreach(t => print(t))
    EsSparkSQL.saveToEs(dataFrame,"people/person")
  }



  class Person1(name: String, surname: String, age: Int)
}