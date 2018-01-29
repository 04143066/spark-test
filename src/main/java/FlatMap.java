import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * flatMap 可以理解成讲返回的迭代器拍扁，将 list 里面的 list 拿出来，转换成一层 list
 * Created by zouxiongxin on 2018/1/26.
 */
public class FlatMap {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("my App");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.parallelize(Arrays.asList("hello world", "hi"));
        JavaRDD<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
        List<String> collect = words.collect();
        collect.forEach(System.out::println);
    }
}
