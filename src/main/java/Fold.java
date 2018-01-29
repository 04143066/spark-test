import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * 和 Reduce 很像，但是 fold 需要指定一个初始值,而且这初始值前后一共算了两遍
 * Created by zouxiongxin on 2018/1/26.
 */
public class Fold {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("app");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
//        JavaRDD<String> rdd = jsc.parallelize(Arrays.asList("a", "b", "c", "d"));
        JavaRDD<Integer> rdd = jsc.parallelize(Arrays.asList(1, 2, 3, 4));
        Integer fold = rdd.fold(10, ((x, y) -> x + y));
        System.out.println(fold);
    }
}
