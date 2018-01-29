import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;
import java.util.Map;

/**
 * 求两个 RDD 的笛卡尔积
 * Created by zouxiongxin on 2018/1/26.
 */
public class Cartesian {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("my App");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> javaRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4));
        JavaRDD<Integer> javaRDD1 = sc.parallelize(Arrays.asList(5, 6, 7, 8));
        JavaPairRDD<Integer, Integer> cartesian = javaRDD.cartesian(javaRDD1);
        // 如果使用 collectAsMap ，当 key 一样就会覆盖， 用 collect 就不会覆盖，都保存下来
        java.util.Map<Integer, Integer> map = cartesian.collectAsMap();
        List<Tuple2<Integer, Integer>> collect = cartesian.collect();
        collect.forEach(System.out::println);
        System.out.println(map.size());
        Set<Map.Entry<Integer, Integer>> entries = map.entrySet();
        entries.forEach(x -> System.out.println(x.getKey() + ":" + x.getValue()));
    }
}
