package com.nju.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

/**
 * @decription: WindowsOperation 一个窗口 包含多个 采集周期
 * @author: qyl
 */
public class SparkStreamingTest08 {
    public static void main(String[] args) throws InterruptedException {
        // 设置环境配置
        SparkConf conf = new SparkConf ( ).setAppName ("streaming").setMaster ("local[*]");
        // 设置批量处理的周期
        try (JavaStreamingContext ssc = new JavaStreamingContext (conf, Durations.seconds (3))) {

            JavaReceiverInputDStream<String> data9999 = ssc.socketTextStream ("localhost", 9999);

            JavaPairDStream<String, Integer> stream9999 = data9999.mapToPair (data -> new Tuple2<> (data, 8));


            // 窗口范围应该是采集周期的整数倍
            // 周期满足了窗口大小才进行统计，窗口可以滑动，默认一个滑动周期是一个采集周期
            JavaPairDStream<String, Integer> window = stream9999.window (Durations.seconds (6));

            // 为了避免过多重复计算，可以改变滑动幅度减小重复数据的范围
            stream9999.window (Durations.seconds (6), Durations.seconds (4));
            window.reduceByKey (Integer::sum).print ( );

            ssc.start ( );
            ssc.awaitTermination ( );
        }
    }
}
