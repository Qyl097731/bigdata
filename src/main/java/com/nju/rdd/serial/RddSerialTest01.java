package com.nju.rdd.serial;

import com.nju.factory.ContextFactory;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.Arrays;

/**
 * @description
 * @date:2022/11/14 23:44
 * @author: qyl
 */
public class RddSerialTest01 {
    public static void main(String[] args) {
        try (JavaSparkContext sc = ContextFactory.create("local")) {
            JavaRDD<String> rdd = sc.parallelize(Arrays.asList("hello world", "hello spark", "hive", "test"));
            Search search = new Search("h");
            search.getMatch1(rdd);
            search.getMatch2(rdd);

        }
    }

    static class Search implements Serializable {
        private String query;

        public Search(String query) {
            this.query = query;
        }


        public boolean isMatch(String s) {
            return s.contains(query);
        }

        // 函数序列化案例 外部函数
        public JavaRDD<String> getMatch1(JavaRDD<String> rdd) {
            return rdd.filter(this::isMatch);
        }

        // 属性序列化案例 匿名函数
        public JavaRDD<String> getMatch2(JavaRDD<String> rdd) {
            return rdd.filter(x -> x.contains(query));
        }

    }
}
