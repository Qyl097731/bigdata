package com.nju.acc;

import com.nju.factory.ContextFactory;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;
import scala.Option;

import java.util.Arrays;

/**
 * @description map延迟加载导致累加器少加 必须要规约；如果多次规约，就会重复累加
 * @date:2022/11/15 21:54
 * @author: qyl
 */
public class SparkAccTest02 {
    public static void main(String[] args) {
        try (JavaSparkContext sc = ContextFactory.create("local")) {
            JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
            LongAccumulator acc = new LongAccumulator();
            acc.register(SparkContext.getOrCreate(), Option.empty(), false);
//            // 错误
//            rdd.map(num->{
//                acc.add(num);
//                return num;
//            });
            // 正确
            rdd.map(num -> {
                acc.add(num);
                return num;
            }).collect();
            System.out.println(acc);
        }
    }
}
