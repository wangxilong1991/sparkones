package wxl.com

import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.sql.EsSparkSQL

/**
  * Created by wangxl6 on 2018/7/25.
  */
object SparkSessionTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test")
      .config("spark.es.nodes", "10.80.139.180")
      .config("spark.es.port", "9201")
      .config("spark.es.nodes.discovery", "ture")
      .config("spark.es.nodes.client.only", "false")
      .config("spark.es.nodes.wan.only", "false")
      .config("spark.es.net.http.auth.user", "elastic")
      .config("spark.es.net.http.auth.pass", "changeme")
      .master("local[1]").getOrCreate();

    val df = spark.read.json("C:\\Users\\wangxl6\\Desktop\\test.json").toDF()
    df.show()
    EsSparkSQL.saveToEs(df,"scala/test")
//    EsSparkSQL.saveToEs(df.map)
//    dataset.show()
//    EsSparkSQL.esDF(spark).
//    val langPercentDF = spark.createDataFrame(List(("Scala", 35), ("Python", 30), ("R", 15), ("Java", 20)))
//    langPercentDF.rdd.saveAsObjectFile()

  }
}
