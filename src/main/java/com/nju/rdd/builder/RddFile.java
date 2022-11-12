package com.nju.rdd.builder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.IOException;

import static com.nju.consts.UrlConstants.PATH;

/**
 * @description
 * @date:2022/11/12 13:56
 * @author: qyl
 */
public class RddFile {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("file");
        try (JavaSparkContext ctx = new JavaSparkContext(conf)) {
            JavaPairRDD<String, String> rdd = ctx.wholeTextFiles(new File(PATH).getParentFile().getCanonicalPath() +
                    File.separator + "*.txt");
            rdd.collect().forEach(System.out::println);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
