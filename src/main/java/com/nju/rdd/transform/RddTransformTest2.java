package com.nju.rdd.transform;

import com.nju.factory.ContextFactory;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @description flatMap 压缩数据
 * @date:2022/11/12 15:44
 * @author: qyl
 */
public class RddTransformTest2 {
    public static void main(String[] args) {
        try (JavaSparkContext sc = ContextFactory.create("local")) {
            JavaRDD<List<Integer>> rdd = sc.parallelize(Arrays.asList(Arrays.asList(1, 2), Arrays.asList(3, 4)));
            JavaRDD<Integer> flatMap = rdd.flatMap(r -> new ArrayList<>(r).iterator());
            flatMap.collect().forEach(System.out::println);

            JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("Hello world", "Hello Spark"));
            JavaRDD<String> flatMap1 = rdd1.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
            flatMap1.collect().forEach(System.out::println);
        }
    }
}
