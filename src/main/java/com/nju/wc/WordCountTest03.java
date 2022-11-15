package com.nju.wc;

import com.nju.factory.ContextFactory;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;

/**
 * @description 统计月份单词
 * @date:2022/11/14 17:38
 * @author: qyl
 */
public class WordCountTest03 {
    public static void main(String[] args) {
        try (JavaSparkContext sc = ContextFactory.create("local")) {
            int sum = 0;

        }
    }

    static class MyComparator implements Serializable, Comparator<Tuple2<String, Integer>> {
        @Override
        public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
            return -Integer.compare(o1._2, o2._2);
        }
    }
}
