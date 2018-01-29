import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.*;
import java.util.Map;

/**
 * Spark 为包含键值对类型的 RDD 提供了一些专有的操作
 * Created by zouxiongxin on 2018/1/26.
 */
public class CreatePairRDD {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("app");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        JavaRDD<String> javaRDD = jsc.parallelize(Arrays.asList("How are you? I'm fine, thank you!", "I love Java!"));
        JavaPairRDD<String, Integer> javaPairRDD = javaRDD.flatMapToPair((PairFlatMapFunction<String, String, Integer>) s -> {
            System.out.println("s -> "+s);
            ArrayList<Tuple2<String, Integer>> list = new ArrayList<>();
            Arrays.asList(s.split(" ")).forEach(x -> list.add(new Tuple2<>(x, 1)));
            return list.iterator();
        });
        JavaPairRDD<String, Integer> javaPairRDD1 = javaRDD.mapToPair(x -> {
            return new Tuple2<>(x.split(" ")[0], 1);
        });
        java.util.Map<String, Integer> map = javaPairRDD.collectAsMap();
        Set<Map.Entry<String, Integer>> entrySet = map.entrySet();
        entrySet.forEach(x -> System.out.println(x.getKey() + " --> " + x.getValue()));
    }
}
