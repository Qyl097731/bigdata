package com.nju.acc;

import com.nju.factory.ContextFactory;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.AccumulatorV2;
import scala.Option;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @description 自定义累加器
 * @date:2022/11/15 21:54
 * @author: qyl
 */
public class SparkAccTest03 {
    public static void main(String[] args) {
        try (JavaSparkContext sc = ContextFactory.create("local")) {
            JavaRDD<String> rdd = sc.parallelize(Arrays.asList("Hello", "Hello", "World"));

            // 创建累加器
            MyAccumulator wcAcc = new MyAccumulator();
            // 向Spark进行注册
            wcAcc.register(SparkContext.getOrCreate(), Option.apply("wordCountAcc"), false);
            // 累加
            rdd.foreach(wcAcc::add);
            System.out.println(wcAcc.value());
        }
    }

    /**
     * 自定义累加器 WordCount
     */
    static class MyAccumulator extends AccumulatorV2<String, Map<String, Integer>> {
        private Map<String, Integer> map = new HashMap<>();

        @Override
        public boolean isZero() {
            return map.isEmpty();
        }

        @Override
        public AccumulatorV2<String, Map<String, Integer>> copy() {
            return new MyAccumulator();
        }

        @Override
        public void reset() {
            map.clear();
        }

        @Override
        public void add(String v) {
            map.put(v, map.getOrDefault(v, 0) + 1);
        }

        @Override
        public void merge(AccumulatorV2<String, Map<String, Integer>> other) {
            Map<String, Integer> value = other.value();
            if (!value.isEmpty()) {
                for (Map.Entry<String, Integer> entry : value.entrySet()) {
                    map.put(entry.getKey(), map.getOrDefault(entry.getKey(), 0) + 1);
                }
            }
        }

        @Override
        public Map<String, Integer> value() {
            return map;
        }
    }
}
