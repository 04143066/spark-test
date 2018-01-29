import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 *  map 把 RDD 中的元素遍历一遍，并作一对一的转换
 * Created by zouxiongxin on 2018/1/25.
 */
public class Map {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("my app");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
        JavaRDD<Integer> map = rdd.map(x -> x * x);
        List<Integer> collect = map.collect();
        collect.forEach(System.out::println);
    }
}
