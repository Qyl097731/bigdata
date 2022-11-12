package com.nju.rdd.builder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * @description 从内存中创建RDD
 * @date:2022/11/12 12:28
 * @author: qyl
 */
public class RddMemory {
    public static void main(String[] args) {
        // 准备环境
        SparkConf conf = new SparkConf().setMaster("local").setAppName("RDD");
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            // 创建RDD
            // 从内存中创建RDD，将内存中集合数据作为处理的数据源
            JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
            rdd.collect().forEach(System.out::println);
        }
    }
}
