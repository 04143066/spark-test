import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * intersection 返回两个 RDD 中都存在的元素
 * Created by zouxiongxin on 2018/1/26.
 */
public class Intersection {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("app");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        JavaRDD<Integer> rdd1 = jsc.parallelize(Arrays.asList(1, 2, 3, 2));
        JavaRDD<Integer> rdd2 = jsc.parallelize(Arrays.asList(2, 3, 4, 2));
        JavaRDD<Integer> intersection = rdd1.intersection(rdd2);
        List<Integer> collect = intersection.collect();
        collect.forEach(System.out::println);
    }
}
