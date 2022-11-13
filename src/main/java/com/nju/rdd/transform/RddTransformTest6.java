package com.nju.rdd.transform;

import com.nju.factory.ContextFactory;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @description filter 进行过滤，分区不变，但是分区内的数据可能不均衡，即数据倾斜（某些分区筛选了过多的数据）
 * @date:2022/11/12 15:44
 * @author: qyl
 */
public class RddTransformTest6 {
    public static void main(String[] args) {
        try (JavaSparkContext sc = ContextFactory.create("local")) {
            JavaRDD<Integer> rdd = sc.parallelize(Stream.of(1, 2, 3, 4).collect(Collectors.toList()), 2);

            // groupBy 将数据源中的每一个数据进行分组判断，根据key进行分组
            // 相同的key放在同一个组中
            rdd.filter(num -> num % 2 == 1).collect().forEach(System.out::println);
        }
    }
}
