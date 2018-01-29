import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;

/**
 * TODO
 * Created by zouxiongxin on 2018/1/25.
 */
public class HelloWorld {
    public static void main(String[] args) {
        //创建一个Java版本的Spark Context
        SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //读取我们的输入数据
        JavaRDD<String> input = sc.textFile("C:\\Users\\zouxiongxin\\Desktop\\QQ.sql");
        //切分为单词
        JavaRDD<String> words = input.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
        JavaPairRDD<String, Integer> wordsMap = words.mapToPair(x -> new Tuple2<>(x, 1));
        JavaPairRDD<String, Integer> counts = wordsMap.reduceByKey((x, y) -> x + y);
        Map<String, Integer> map = counts.collectAsMap();
        for (String s : map.keySet()) {
            System.out.println(s);
        }
        counts.saveAsTextFile("D:\\test\\e");
    }
}
