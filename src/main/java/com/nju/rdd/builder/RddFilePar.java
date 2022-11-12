package com.nju.rdd.builder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;

import static com.nju.consts.UrlConstants.PATH;

/**
 * @description 默认最小分区2
 * @date:2022/11/12 14:57
 * @author: qyl
 */
public class RddFilePar {
    public static void main(String[] args) {
        // 准备环境
        SparkConf conf = new SparkConf().setMaster("local").setAppName("RDD");
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {

            // 采用hadoop方式读取，行读取
            // 数据读取的时候以偏移量为单位
            // 分区偏移量范围计算（读多少）
            JavaRDD<String> rdd = sc.textFile(PATH, 3);
            rdd.saveAsTextFile(new File(PATH).getParent() + "/output");
        }
    }
}
