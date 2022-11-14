package com.nju.rdd.transform;

import com.nju.factory.ContextFactory;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @description 优化RddTransformTest17, 把第一个作为初始值 combineByKey
 * @author: qyl
 */
public class RddTransformTest19 {
    public static void main(String[] args) {
        try (JavaSparkContext sc = ContextFactory.create("local")) {

            JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(Arrays.asList(new Tuple2("a", 1), new Tuple2("a", 5),
                    new Tuple2(
                            "a", 8),
                    new Tuple2("b", 1), new Tuple2("b", 2), new Tuple2("b", 3)), 2);
            rdd1.combineByKey(
                    v1 -> new Tuple2<>(v1, 1),
                    (t1, t2) -> new Tuple2<>(t1._1 + t2, t1._2 + 1),
                    (t1, t2) -> new Tuple2<>(t1._1 + t2._1, t1._2 + t2._1)
            ).mapValues(t -> new Tuple2<>(t._1, t._1 * 0.1 / t._2)).collect().forEach(System.out::println);
        }
    }
}
