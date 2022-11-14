package com.nju.rdd.transform;

import com.nju.factory.ContextFactory;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import static java.util.Arrays.asList;

/**
 * @description join 把所有相同key的v整合在一起,笛卡尔积导致数据几何增长 性能急剧下降
 * @author: qyl
 */
public class RddTransformTest20 {
    public static void main(String[] args) {
        try (JavaSparkContext sc = ContextFactory.create("local")) {

            JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(asList(new Tuple2("a", 1), new Tuple2("a", 5), new Tuple2("a", 8)), 2);
            JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(asList(new Tuple2("a", 1), new Tuple2("b", 2),
                    new Tuple2("b", 3)), 2);
            rdd.join(rdd1).collect().forEach(System.out::println);
        }
    }
}
