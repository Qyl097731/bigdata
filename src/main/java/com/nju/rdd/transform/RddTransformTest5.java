package com.nju.rdd.transform;

import com.nju.factory.ContextFactory;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @description groupby 分组 将原本分好区的数据进行重组，不一定原来在一起的还在一起了，一个分区不一定只有一个组
 * @date:2022/11/12 15:44
 * @author: qyl
 */
public class RddTransformTest5 {
    public static void main(String[] args) {
        try (JavaSparkContext sc = ContextFactory.create("local")) {
            JavaRDD<Integer> rdd = sc.parallelize(Stream.of(1, 2, 3, 4).collect(Collectors.toList()), 2);

            // groupBy 将数据源中的每一个数据进行分组判断，根据key进行分组
            // 相同的key放在同一个组中
            rdd.groupBy(num -> num % 2).collect().forEach(System.out::println);

            // 相同的首字母分在一组
            JavaRDD<String> rdd1 =
                    sc.parallelize(Stream.of("Hello", "World", "Ketty", "Haha", "Wow").collect(Collectors.toList()), 2);

            rdd1.groupBy(s -> s.charAt(0)).collect().forEach(System.out::println);

        }
    }
}
