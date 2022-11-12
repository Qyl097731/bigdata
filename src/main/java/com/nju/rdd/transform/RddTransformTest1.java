package com.nju.rdd.transform;

import com.nju.factory.ContextFactory;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * @description 直接对分区进行操作
 * @date:2022/11/12 15:44
 * @author: qyl
 */
public class RddTransformTest1 {
    public static void main(String[] args) {
        try (JavaSparkContext sc = ContextFactory.create("local")) {
            // 对每个分区进行一次操作，但是会一直保存对分区的使用,直到分区内的所有计算结束，内存小，数据量大的时候会出现内存溢出
            JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4), 2);

            rdd = rdd.mapPartitions(p -> {
                System.out.println(">>>>>>");
                return p;
            }).map(num -> num * 2);
            rdd.collect().forEach(System.out::println);
        }
    }
}
