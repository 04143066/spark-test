import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * 移除一个 RDD 中另一个 RDD 存在的内容
 * Created by zouxiongxin on 2018/1/26.
 */
public class Subtract {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("my App");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> javaRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4));
        JavaRDD<Integer> javaRDD1 = sc.parallelize(Arrays.asList(3, 4, 5));
        JavaRDD<Integer> subtract = javaRDD.subtract(javaRDD1);
        List<Integer> collect = subtract.collect();
        collect.forEach(System.out::println);
    }
}
