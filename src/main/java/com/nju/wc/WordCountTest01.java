package com.nju.wc;

import com.nju.factory.ContextFactory;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * @description 统计月份单词
 * @date:2022/11/14 17:38
 * @author: qyl
 */
public class WordCountTest01 {
    public static void main(String[] args) {
        try (JavaSparkContext sc = ContextFactory.create("local")) {
            long start = System.currentTimeMillis();
            sc.textFile("data/*.csv").zipWithIndex()
                    .filter(t -> t._2 % 2 == 1)
                    .map(t -> t._1.split(","))
                    .mapToPair(t -> new Tuple2<>(t[1], t[3]))
                    .flatMapValues(s -> Arrays.asList(s.split(",")).iterator())
                    .mapToPair(t -> new Tuple2<>(new Tuple2<>(t._1, t._2), 1))
                    .reduceByKey(Integer::sum)
                    .mapToPair(t -> new Tuple2<>(t._1._1, new Tuple2<>(t._1._2, t._2)))
                    .groupByKey()
                    .mapValues(item -> StreamSupport.stream(item.spliterator(),
                            false).sorted(new MyComparator()).limit(3).collect(Collectors.toList()))
                    .collect().forEach(System.out::println);


            long end = System.currentTimeMillis();
            System.out.println((end - start) / 1000);
        }
    }

    static class MyComparator implements Serializable, Comparator<Tuple2<String, Integer>> {
        @Override
        public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
            return -Integer.compare(o1._2, o2._2);
        }
    }
}
