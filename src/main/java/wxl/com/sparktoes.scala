package wxl.com

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark
/**
  * Created by wangxl6 on 2018/7/23.
  */
object sparktoes {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("es").setMaster("local[1]")
    //    val conf = new SparkConf()

    conf.set("spark.es.nodes", "10.80.139.180")
    conf.set("spark.es.port", "9201")
    conf.set("spark.es.nodes.discovery", "ture")
    conf.set("spark.es.nodes.client.only", "false")
    conf.set("spark.es.nodes.wan.only", "false")
    conf.set("spark.es.net.http.auth.user", "elastic")
    conf.set("spark.es.net.http.auth.pass", "changeme")
    //    conf.set("es.index.auto.create", "true")
    val sc = new SparkContext(conf)
    //    createindex(sc)
    //    search(sc,"spark/docs")
//        search_then_load(sc,"json/test")
    //    load_json(sc,args(0))
//    val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
//    val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")
//    var esRDD = sc.makeRDD(Seq(numbers,airports))
//    EsSpark.saveToEs(esRDD,"sparkcontext/test")
    EsSpark.saveJsonToEs(sc.textFile("C:\\Users\\wangxl6\\Desktop\\test.json"),"sparkcontext/test")
//    val esRDD = sc.parallelize(1 to 100).map(t => println(t))).toDF();
//    esRDD.save
//    mapRDD.foreach(t=> println(t))

//    mapRDD.sa

//    print
//      def createindex( sc : SparkContext ): Unit ={
//
//        val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
//        val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")
//        sc.makeRDD(Seq(numbers, airports)).saveToEs("spark/docs")
//
//      }
//
//      def search(sc : SparkContext,index : String): Unit ={
//        //    val resource = index
//        val accountRDD = sc.esJsonRDD(index)
//        accountRDD.foreach(t => println(t._1 + "===" +t._2))
//      }
//
//      def search_then_load(sc : SparkContext,index : String): Unit ={
//        //    val resource = index
//        val accountRDD = sc.esJsonRDD(index)
//        //    accountRDD.foreach(t => println(t._1 + "===" +t._2))
//        accountRDD.saveToEs("json1/test")
//      }
//
//      def load_json(sc : SparkContext,path : String): Unit ={
//        val aLinesRDD = sc.textFile(path).collect()
//        val resource = "json/test"
//        sc.makeRDD(aLinesRDD).saveJsonToEs(resource)
//
//      }
  }
}
