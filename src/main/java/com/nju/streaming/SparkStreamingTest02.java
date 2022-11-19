package com.nju.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @date:2022/11/17 23:59
 * @author: qyl
 */
public class SparkStreamingTest02 {
    public static void main(String[] args) {
        // 设置环境配置
        SparkConf conf = new SparkConf ( ).setAppName ("streaming").setMaster ("local[*]");
        // 设置批量处理的周期
        try (JavaStreamingContext ssc = new JavaStreamingContext (conf, Durations.seconds (3))) {
            Deque<JavaRDD<Integer>> rddQueue = new ArrayDeque<> ( );
            JavaInputDStream<Integer> inputDStream = ssc.queueStream (rddQueue, false);
            JavaPairDStream<Integer, Integer> pair = inputDStream.mapToPair (num -> new Tuple2<> (num, 1));
            pair.reduceByKey (Integer::sum).print ( );

            ssc.start ( );
            for (int i = 0; i < 5; i++) {
                rddQueue.offerLast (ssc.sparkContext ( ).parallelize (IntStream.rangeClosed (1, 300).boxed ( ).collect (Collectors.toList ( )), 10));
                Thread.sleep (3000);
            }
            ssc.awaitTermination ( );
        } catch (InterruptedException e) {
            e.printStackTrace ( );
        }
    }
}
