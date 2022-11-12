package com.nju.rdd.transform;

import com.nju.factory.ContextFactory;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * @description flatMap 把数据进行压缩
 * @date:2022/11/12 15:44
 * @author: qyl
 */
public class RddTransformTest3 {
    public static void main(String[] args) {
        try (JavaSparkContext sc = ContextFactory.create("local")) {
            JavaRDD<Object> rdd = sc.parallelize(Arrays.asList(Arrays.asList(1, 2), Arrays.asList(3, 4), 5));
            rdd.flatMap(data -> {
                if (data instanceof List) {
                    return ((List) data).iterator();
                }
                return Arrays.asList(data).iterator();
            }).collect().forEach(System.out::println);
        }
    }
}
