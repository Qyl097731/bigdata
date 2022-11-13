package com.nju.rdd.transform;

import com.nju.factory.ContextFactory;
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @description k-v类型 分区器 HashPartitioner
 * @date:2022/11/12 15:44
 * @author: qyl
 */
public class RddTransformTest14 {
    public static void main(String[] args) {
        try (JavaSparkContext sc = ContextFactory.create("local")) {
            JavaRDD<Integer> rdd1 =
                    sc.parallelize(IntStream.rangeClosed(1, 5).boxed().collect(Collectors.toList()), 4);
            JavaPairRDD<Integer, Integer> pair = rdd1.mapToPair(item -> new Tuple2<>(item, 1));
            pair.partitionBy(new HashPartitioner(2)).saveAsTextFile("data/output");
        }
    }
}
