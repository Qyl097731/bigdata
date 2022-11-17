package com.nju.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @date:2022/11/17 23:59
 * @author: qyl
 */
public class SparkStreamingTest01 {
    public static void main(String[] args) {
        // 设置环境配置
        SparkConf conf = new SparkConf().setAppName("streaming").setMaster("local[*]");
        // 设置批量处理的周期
        try (JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(3))) {

            // 获取端口数据
            JavaReceiverInputDStream<String> lines = ssc.socketTextStream("localhost", 9999);

            // 数据处理
            JavaDStream<String> stream = lines.flatMap(s ->
                    Arrays.asList(s.split(",")).iterator());
            JavaPairDStream<String, Integer> pair = stream.mapToPair(w -> new Tuple2<>(w, 1));
            pair.reduceByKey(Integer::sum).print();

            // 采集器开启
            ssc.start();
            ssc.awaitTermination();
//            ssc.stop(true);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
