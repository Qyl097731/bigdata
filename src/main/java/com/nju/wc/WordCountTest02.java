package com.nju.wc;

import com.nju.factory.ContextFactory;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
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
public class WordCountTest02 {
    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        try (JavaSparkContext sc = ContextFactory.create("local")) {
            SparkSession spark = SparkSession.builder()
                    .appName("cloud")
                    .config("spark.some.config.option", "some-value")
                    .getOrCreate();
            Dataset<Row> dateSet = spark.read().option("delimiter", ",").option("header", "true").csv("data/all_data.csv")
                    .select("date", "keyword").cache();
            JavaRDD<Row> rdd = dateSet.toJavaRDD();

            rdd.mapToPair(row -> {
                String[] split = row.getString(0).split("[^a-zA-Z0-9]");
                return new Tuple2<>(split[split.length - 1], row.getString(1).split(","));
            }).flatMapValues(v -> Arrays.asList(v).iterator())
                    .mapToPair(t -> new Tuple2<>(new Tuple2<>(t._1, t._2), 1))
                    .reduceByKey(Integer::sum)
                    .mapToPair(t -> new Tuple2<>(t._1._1, new Tuple2<>(t._1._2, t._2)))
                    .groupByKey()
                    .mapValues(item -> StreamSupport.stream(item.spliterator(),
                            false).sorted(new MyComparator()).limit(15).collect(Collectors.toList()))
                    .sortByKey()
                    .collect().forEach(g -> {
                System.out.println(g);
                System.out.println();
            });
        }
        long end = System.currentTimeMillis();
        System.out.println((end - start) / 1_000);
    }

    static class MyComparator implements Serializable, Comparator<Tuple2<String, Integer>> {
        @Override
        public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
            return -Integer.compare(o1._2, o2._2);
        }
    }
}
