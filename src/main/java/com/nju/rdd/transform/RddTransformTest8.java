package com.nju.rdd.transform;

import com.nju.factory.ContextFactory;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @description coalesce 1.缩减分区,分区数据筛选之后合并成一个分区，减少任务调度成本
 * 但是减少分区之后，缩减分区可能导致数据不均衡
 * 可以通过shuffle进行重组，让数据均衡
 * 2.扩大分区 必须要shuffle为true
 * @date:2022/11/12 15:44
 * @author: qyl
 */
public class RddTransformTest8 {
    public static void main(String[] args) {
        try (JavaSparkContext sc = ContextFactory.create("local")) {
            JavaRDD<Integer> rdd =
                    sc.parallelize(IntStream.rangeClosed(1, 10).boxed().collect(Collectors.toList()),
                            5);  // 五个分区
            // 两个分区
            rdd = rdd.coalesce(2, true);
            rdd.saveAsTextFile("data/output1");

            // 扩大分区
            rdd = rdd.coalesce(10, true);
            rdd.saveAsTextFile("data/output2");

        }
    }
}
