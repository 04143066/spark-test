import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * reduce 是行动操作，将 RDD 中的元素转换成一个数
 * Created by zouxiongxin on 2018/1/26.
 */
public class Reduce {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("app");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        JavaRDD<Integer> rdd = jsc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
        Integer reduce = rdd.reduce((x, y) -> x + y);
        System.out.println(reduce);
    }
}
