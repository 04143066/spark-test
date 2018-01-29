import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * union 返回两个 RDD 中都存在的元素
 * Created by zouxiongxin on 2018/1/26.
 */
public class Union {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("myApp");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        JavaRDD<Integer> rdd = jsc.parallelize(Arrays.asList(1, 2, 3, 1));
        JavaRDD<Integer> rdd1 = jsc.parallelize(Arrays.asList(3, 4, 5));
        JavaRDD<Integer> union = rdd.union(rdd1);
        List<Integer> collect = union.collect();
        collect.forEach(System.out::println);
    }
}
