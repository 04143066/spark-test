import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import java.util.Arrays;

/**
 * 在迭代算法中，经常会多次用同一种数据，为了避免多次计算同一个 RDD,就持久化一下
 * 但是他在第一次调用的时候不会强制求值
 * Created by zouxiongxin on 2018/1/26.
 */
public class Persist {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("app");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        JavaRDD<String> rdd = jsc.parallelize(Arrays.asList("1,2,3"));
        JavaRDD<String> persist = rdd.persist(StorageLevel.MEMORY_AND_DISK());
        // unpersisit() 可以将持久化的 RDD 从缓存中移除
    }
}
