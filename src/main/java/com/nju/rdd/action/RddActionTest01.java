package com.nju.rdd.action;

import com.nju.factory.ContextFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.System.out;

/**
 * @description reduce
 * @author: qyl
 */
public class RddActionTest01 {
    public static void main(String[] args) {
        try (JavaSparkContext sc = ContextFactory.create("local")) {
            JavaRDD<Integer> rdd = sc.parallelize(Stream.of(1, 2, 3, 4).collect(Collectors.toList()));

            // reduce 规约
            out.println(rdd.reduce(Integer::sum));

            // collect 从分区中顺序采集到内存中形成数组
            out.println(StringUtils.join(rdd.collect().toArray(new Integer[0]), ","));

            // count 数据源中数据的个数
            out.println(rdd.count());

            // first获取数据源中的第一个
            out.println(rdd.first());

            // take 获取n个数据
            out.println(rdd.take(3));
            rdd = sc.parallelize(Stream.of(3, 2, 5, 0).collect(Collectors.toList()));
            // takeOrOrdered
            List<Integer> list = rdd.takeOrdered(3, new MyComparator());

            out.println(list);

            // aggregate 聚合统计 初始值在分区间都会起作用
            out.println(rdd.aggregate(0, Integer::sum, Integer::sum));

            // fold 分区间 和 分区内操作相同的时候
            out.println(rdd.fold(0, Integer::sum));

            // countByValue
            out.println(rdd.countByValue());
        }
    }

    static class MyComparator implements Serializable, Comparator<Integer> {
        @Override
        public int compare(Integer o1, Integer o2) {
            return -Integer.compare(o1, o2);
        }
    }
}
