import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.*;


public class SparkHelloWorld {
    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext("local", "SparkHelloWorld");

        List<String> data2List1 = new ArrayList<String>();
        data2List1.add("1,2,3,4,5");
        data2List1.add("a,b,c,d,e");

        JavaRDD<String> list1 = sc.parallelize(data2List1);

        List<String> data2List = new ArrayList<String>();
        data2List.add("11,22,33,44,55");
        data2List.add("aa,bb,cc,dd,ee");
        JavaRDD<String> list2 = sc.parallelize(data2List);



        // Map
        JavaRDD<String[]> mapRdd = list1.map(
                new Function<String, String[]>() {
                    @Override
                    public String[] call(String s) throws Exception {
                        return s.split(",");
                    }
                }
        );
        mapRdd.foreach(
                new VoidFunction<String[]>() {
                    @Override
                    public void call(String[] strings) throws Exception {
                        Collections.
                        System.out.println("strings = [" + strings + "]");
                    }
                }
        );

        //flatMap
        JavaRDD<String> flatMapRdd = list1.flatMap(
                new FlatMapFunction<String, String>() {
                    @Override
                    public Iterator<String> call(String s) throws Exception {
                        return Arrays.asList(s.split(",")).iterator();
                    }
                }
        );

        //filter
        JavaRDD<String> filterRdd = list2.filter(
                new Function<String, Boolean>() {
                    @Override
                    public Boolean call(String s) throws Exception {
                        if(s.contains("s"))
                            return true;
                        return false;
                    }
                }
        );


        //union
        JavaRDD<String> unionRdd = list1.union(list2);


        // groupByKey
        List<Tuple2<String, String>> pair1 = new ArrayList<>();
        pair1.add(new Tuple2<String, String>("a", "1"));
        pair1.add(new Tuple2<String, String>("b", "2"));
        pair1.add(new Tuple2<String, String>("c", "3"));
        pair1.add(new Tuple2<String, String>("d", "4"));

        JavaPairRDD<String, String> pairRDD = sc.parallelizePairs(pair1);

        JavaPairRDD<String, Iterable<String>> groupByKeyRdd = pairRDD.groupByKey();


        // reduceByKey
        JavaPairRDD<String, String> reduceByKeyRDD = pairRDD.reduceByKey(
                new Function2<String, String, String>() {
                    @Override
                    public String call(String s, String s2) throws Exception {
                        return s + "|" + s2;
                    }
                }
        );

        //mapValues
        JavaPairRDD<String, String> mapValueRDD = pairRDD.mapValues(
                new Function<String, String>() {
                    @Override
                    public String call(String s) throws Exception {
                        return s + "test";
                    }
                }
        );

        // join
        List<Tuple2<String, String>> pair2 = new ArrayList<>();
        pair2.add(new Tuple2<String, String>("a", "11"));
        pair2.add(new Tuple2<String, String>("b", "22"));
        pair2.add(new Tuple2<String, String>("c", "33"));
        pair2.add(new Tuple2<String, String>("d", "44"));

        JavaPairRDD<String, String> pair2RDD = sc.parallelizePairs(pair1);
        JavaPairRDD<String, Tuple2<String, String>> joinRDD = pairRDD.join(pair2RDD);

        // cogroup
        JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> cogroup = pairRDD.cogroup(pair2RDD);

        sc.stop();
    }
}
