package wxl.com;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;

/**
 * Created by wangxl6 on 2018/7/25.
 */
public class SparkSqlToEs {
    public static void main(String[] args) {
        SparkSession spark=SparkSession.builder().appName("Sql2Es")
                .config("spark.es.nodes", "10.80.139.180")
                .config("spark.es.port", "9201")
                .config("spark.es.nodes.discovery", "ture")
                .config("spark.es.nodes.client.only", "false")
                .config("spark.es.nodes.wan.only", "false")
                .config("spark.es.net.http.auth.user", "elastic")
                .config("spark.es.net.http.auth.pass", "changeme")
                .master("local[5]").getOrCreate();
        long l = System.currentTimeMillis();
        Dataset<Row> person=spark.read().json("C:\\Users\\wangxl6\\Desktop\\test.json");
        long l1 = System.currentTimeMillis();
        person.show();
        long l2 = System.currentTimeMillis();
        JavaEsSparkSQL.saveToEs(person,"test/json");
        long l3 = System.currentTimeMillis();
        System.out.println((l1 -l)+":"+(l2-l1)+":"+(l3-l2));
    }
}
