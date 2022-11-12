package com.nju.rdd.transform;

import com.nju.factory.ContextFactory;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * @description 每个数据都进行操作
 * @date:2022/11/12 15:44
 * @author: qyl
 */
public class RddTransformTest {
    public static void main(String[] args) {
        try (JavaSparkContext sc = ContextFactory.create("local")) {
            // 分区之间并行，无顺序，分区内有序
            JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));

            JavaRDD<Integer> rdd1 = rdd.map(num -> {
                System.out.println(">>>>>>" + num);
                return num;
            });

            JavaRDD<Integer> rdd2 = rdd1.map(num -> {
                System.out.println("#######" + num);
                return num;
            });
            rdd2.collect();
        }
    }
}
