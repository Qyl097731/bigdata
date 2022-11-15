package com.nju.acc;

import com.nju.factory.ContextFactory;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;
import scala.Option;

import java.util.Arrays;

/**
 * @description 累加器
 * @date:2022/11/15 21:54
 * @author: qyl
 */
public class SparkAccTest01 {
    public static void main(String[] args) {
        try (JavaSparkContext sc = ContextFactory.create("local")) {
            JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
            LongAccumulator acc = new LongAccumulator();
            acc.register(SparkContext.getOrCreate(), Option.empty(), false);
            rdd.foreach(acc::add);
            System.out.println(acc);
        }
    }
}
