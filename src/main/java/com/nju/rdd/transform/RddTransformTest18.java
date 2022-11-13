package com.nju.rdd.transform;

import com.nju.factory.ContextFactory;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @description foldByKey 如果分区内和分区间操作都相同就是用该聚合操作
 * @date:2022/11/12 15:44
 * @author: qyl
 */
public class RddTransformTest18 {
    public static void main(String[] args) {
        try (JavaSparkContext sc = ContextFactory.create("local")) {

            JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(Arrays.asList(new Tuple2("a", 1), new Tuple2("a", 5),
                    new Tuple2(
                            "a", 8),
                    new Tuple2("b", 1), new Tuple2("a", 2), new Tuple2("a", 3)), 2);
            System.out.println(rdd1.foldByKey(0, Integer::max).collect());
        }
    }
}
