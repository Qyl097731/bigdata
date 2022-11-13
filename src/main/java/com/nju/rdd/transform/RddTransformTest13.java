package com.nju.rdd.transform;

import com.nju.factory.ContextFactory;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @description zip 分区数量要一致且每个分区的元素数量要一致
 * @date:2022/11/12 15:44
 * @author: qyl
 */
public class RddTransformTest13 {
    public static void main(String[] args) {
        try (JavaSparkContext sc = ContextFactory.create("local")) {
            JavaRDD<Integer> rdd1 =
                    sc.parallelize(IntStream.rangeClosed(1, 5).boxed().collect(Collectors.toList()), 4);

            JavaRDD<Integer> rdd2 =
                    sc.parallelize(IntStream.rangeClosed(3, 7).boxed().collect(Collectors.toList()), 2);
            rdd2.zip(rdd1).collect().forEach(System.out::println);
        }
    }
}
