package com.nju.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

/**
 * @decription: 窗口和关键字进行统计
 * @author: qyl
 */
public class SparkStreamingTest10 {
    public static void main(String[] args) throws InterruptedException {
        // 设置环境配置
        SparkConf conf = new SparkConf ( ).setAppName ("streaming").setMaster ("local[*]");
        // 设置批量处理的周期
        try (JavaStreamingContext ssc = new JavaStreamingContext (conf, Durations.seconds (3))) {

            JavaReceiverInputDStream<String> data9999 = ssc.socketTextStream ("localhost", 9999);

            JavaPairDStream<String, Integer> stream9999 = data9999.mapToPair (data -> new Tuple2<> (data, 8));

            stream9999.reduceByKeyAndWindow (
                    Integer::sum,    // 新采集的数据加上去
                    (x, y) -> x - y,     // 覆盖的数据剪掉
                    Seconds.apply (9),  // 窗口数据
                    Seconds.apply (3)); // 滑动频率，也就是说6s是重复数据

            ssc.start ( );
            ssc.awaitTermination ( );
        }
    }

}
