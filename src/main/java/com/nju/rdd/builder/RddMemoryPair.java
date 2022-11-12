package com.nju.rdd.builder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.util.Arrays;

import static com.nju.consts.UrlConstants.PATH;

/**
 * @date:2022/11/12 14:22
 * @author: qyl
 */
public class RddMemoryPair {

    public static void main(String[] args) {
        // 准备环境
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("RDD");
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            // 控制分区
            JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4), 2);
            rdd.saveAsTextFile(new File(PATH).getParent() + File.separator + "output");
            sc.stop();
        }

    }
}
