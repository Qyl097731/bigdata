package com.nju.rdd.part;

import com.nju.factory.ContextFactory;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @description
 * @date:2022/11/15 18:07
 * @author: qyl
 */
public class RddPartTest01 {
    public static void main(String[] args) {
        try (JavaSparkContext sc = ContextFactory.create("local")) {
            JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(Arrays.asList(new Tuple2<>("nba", 1), new Tuple2<>("cba", 2),
                    new Tuple2<>("wnba", 3), new Tuple2<>("nba", 1)), 3);
            MyPartitioner partitioner = new MyPartitioner();
            rdd.partitionBy(partitioner).saveAsTextFile("output");
        }
    }

    static class MyPartitioner extends Partitioner {

        @Override
        public int numPartitions() {
            return 3;
        }

        // 返回key数据的分区索引
        @Override
        public int getPartition(Object key) {
            if (key.equals("nba")) {
                return 0;
            } else if (key.equals("cba")) {
                return 1;
            }
            return 2;
        }
    }
}
