package com.nju.rdd.transform;

import com.nju.factory.ContextFactory;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import static java.util.Arrays.asList;

/**
 * @description cogroup(分组连接 ） 相同key放在一个组 类似于组内相同key先connet ， 之后组件相同key外连接
 * @author: qyl
 */
public class RddTransformTest22 {
    public static void main(String[] args) {
        try (JavaSparkContext sc = ContextFactory.create("local")) {

            JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(asList(new Tuple2("a", 1), new Tuple2("a", 5),
                    new Tuple2("c", 8)), 2);
            JavaPairRDD<String, Integer> rdd2 = sc.parallelizePairs(asList(new Tuple2("a", 1), new Tuple2("b", 2),
                    new Tuple2("b", 3)), 2);
            rdd1.cogroup(rdd2).collect().forEach(System.out::println);
        }
    }
}
