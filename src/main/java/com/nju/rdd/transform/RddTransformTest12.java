package com.nju.rdd.transform;

import com.nju.factory.ContextFactory;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @description 集合操作
 * @date:2022/11/12 15:44
 * @author: qyl
 */
public class RddTransformTest12 {
    public static void main(String[] args) {
        try (JavaSparkContext sc = ContextFactory.create("local")) {
            JavaRDD<Integer> rdd1 =
                    sc.parallelize(IntStream.rangeClosed(1, 5).boxed().collect(Collectors.toList()));

            JavaRDD<Integer> rdd2 =
                    sc.parallelize(IntStream.rangeClosed(3, 7).boxed().collect(Collectors.toList()));
            // 交集
            rdd2.intersection(rdd1).collect().forEach(System.out::println);
            // 并集
            rdd2.union(rdd1).collect().forEach(System.out::println);
            // 差集
            rdd2.subtract(rdd1).collect().forEach(System.out::println);
            // 拉链
            rdd2.zip(rdd1).collect().forEach(System.out::println);
        }
    }
}
