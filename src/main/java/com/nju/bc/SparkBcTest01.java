package com.nju.bc;

import com.nju.factory.ContextFactory;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @description 广播
 * @date:2022/11/15 23:15
 * @author: qyl
 */
public class SparkBcTest01 {
    public static void main(String[] args) {
        try (JavaSparkContext sc = ContextFactory.create("local")) {
            JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(Arrays.asList(new Tuple2<>("a", 1), new Tuple2<>(
                    "b", 2), new Tuple2<>("c", 3)));

            // join 会出现笛卡尔积 数据出现几何增长，影响shuffle 不推荐使用
            JavaPairRDD<String, Integer> rdd2 = sc.parallelizePairs(Arrays.asList(new Tuple2<>("a", 4), new Tuple2<>(
                    "b", 5), new Tuple2<>("c", 6)));
            rdd1.join(rdd2).collect().forEach(System.out::println);

            // map 但是当数据量过大的时候 每个任务都执行map  即每个分区间map必须共享 ，那么所有涉及到该map的都必须存一份map 存储空间浪费
            Map<String, Integer> map = new HashMap<String, Integer>() {{
                put("a", 4);
                put("b", 5);
                put("c", 6);
            }};
            rdd1.map(t -> new Tuple2<>(t._1, new Tuple2<>(t._2, map.getOrDefault(t._1, 0)))).collect().forEach(System.out::println);
        }
    }
}
