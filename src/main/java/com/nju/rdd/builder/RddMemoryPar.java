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
public class RddMemoryPar {
    public static void main(String[] args) {
        // 准备环境
        SparkConf conf = new SparkConf().setMaster("local").setAppName("RDD");
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            // 默认的最小分区为2，这里设置成3，实际情况下会自行调整：
            // 通过totalSize / goalSize 最后有没有余数决定要不要 minPartitions + 1 ，如果余数超过了每个分区的11%就要 分区++
            // goalSize = totalSize / minPartitions ，即每个分区放goalSize个字节
            JavaRDD<String> rdd = sc.textFile(PATH, 3);
            rdd.saveAsTextFile(new File(PATH).getParent() + "/output");
        }
    }
}
