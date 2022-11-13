package com.nju.rdd.transform;

import com.nju.factory.ContextFactory;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @description repartition 扩大分区
 * @date:2022/11/12 15:44
 * @author: qyl
 */
public class RddTransformTest9 {
    public static void main(String[] args) {
        try (JavaSparkContext sc = ContextFactory.create("local")) {
            JavaRDD<Integer> rdd =
                    sc.parallelize(IntStream.rangeClosed(1, 10).boxed().collect(Collectors.toList()),
                            2);  // 五个分区
            // 扩大分区 底层是coalesce
            rdd = rdd.repartition(5);

            rdd.saveAsTextFile("data/output");

        }
    }
}
