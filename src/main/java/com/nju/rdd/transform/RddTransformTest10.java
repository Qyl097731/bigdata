package com.nju.rdd.transform;

import com.nju.factory.ContextFactory;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @description sortBy 排序
 * @date:2022/11/12 15:44
 * @author: qyl
 */
public class RddTransformTest10 {
    public static void main(String[] args) {
        try (JavaSparkContext sc = ContextFactory.create("local")) {

            JavaRDD<Tuple2> rdd = sc.parallelize(Arrays.asList(new Tuple2("1", 1), new Tuple2("11", 2), new Tuple2("3", 3)),
                    2);
            JavaRDD<Tuple2> sortBy = rdd.sortBy(t -> Integer.parseInt(t._1.toString()), true, 1);
            sortBy.saveAsTextFile("data/output");
            // 原来的分区数量不会变
            rdd.saveAsTextFile("data/output1");

        }
    }
}
