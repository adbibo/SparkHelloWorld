import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class SparkOnHive {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("HelloWorld").setMaster("local").setSparkHome("/usr/local/spark");
        // setMaster指定Master
        // setSparkHome指向安装spark的地址，视环境而定
        JavaSparkContext sc = new JavaSparkContext(conf);
        String path = "README.md";

        JavaRDD<String> textFile = sc.textFile(path);
        JavaPairRDD<String, Integer> counts = textFile
                .flatMap(s -> Arrays.asList(s.split(",")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);

        List<Tuple2<String, Integer>> output = counts.collect();

        for (Tuple2<?,?> tuple: output) {
            System.out.println(tuple._1() + ":" + tuple._2());
        }

        sc.close();
    }

}
