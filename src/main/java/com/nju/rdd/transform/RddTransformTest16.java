package com.nju.rdd.transform;

import com.nju.factory.ContextFactory;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @description groupByKey 把v会提出来 key作为组索引
 * @date:2022/11/12 15:44
 * @author: qyl
 */
public class RddTransformTest16 {
    public static void main(String[] args) {
        try (JavaSparkContext sc = ContextFactory.create("local")) {
            JavaRDD<Integer> rdd = sc.parallelize(IntStream.of(1, 2, 1, 10).boxed().collect(Collectors.toList()));
            rdd.mapToPair(item -> new Tuple2<>(item, 1)).groupByKey()
                    .collect().forEach(System.out::println);
        }
    }
}
