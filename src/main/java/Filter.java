import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * 遍历 RDD 中的元素，过滤出想要的元素
 * Created by zouxiongxin on 2018/1/26.
 */
public class Filter {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("my App");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8));
        JavaRDD<Integer> filter = rdd.filter(x -> x % 2 != 0);
        List<Integer> collect = filter.collect();
        collect.forEach(System.out::println);
    }
}
