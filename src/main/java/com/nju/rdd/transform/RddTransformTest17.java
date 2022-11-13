package com.nju.rdd.transform;

import com.nju.factory.ContextFactory;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @description aggregateByKey 分区内，分区间进行柯里化操作，
 * @date:2022/11/12 15:44
 * @author: qyl
 */
public class RddTransformTest17 {
    public static void main(String[] args) {
        try (JavaSparkContext sc = ContextFactory.create("local")) {
            JavaRDD<Integer> rdd = sc.parallelize(IntStream.of(1, 2, 1, 10).boxed().collect(Collectors.toList()), 2);
            System.out.println(rdd.aggregate(0, Integer::max, Integer::sum));


            JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(Arrays.asList(new Tuple2("a", 1), new Tuple2("a", 5),
                    new Tuple2(
                            "a", 8),
                    new Tuple2("b", 1), new Tuple2("a", 2), new Tuple2("a", 3)), 2);
            System.out.println(rdd1.aggregateByKey(5, Integer::max, Integer::sum).collect());

            // 统计多项数据 和 以及 次数
            JavaPairRDD<String, Tuple2<Integer, Integer>> rdd2 = rdd1.aggregateByKey(new Tuple2<>(0, 0),
                    (t, v) -> new Tuple2<>(t._1 + v, t._2 + 1),
                    (t1, t2) -> new Tuple2<>(t1._1 + t2._1, t1._2 + t2._2)
            );
            System.out.println(rdd2.mapValues(t ->
                    new Tuple2<>(t._1, t._1 / 1.0 / t._2)
            ).collect());
        }
    }
}
