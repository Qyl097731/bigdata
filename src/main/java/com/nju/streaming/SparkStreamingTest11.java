package com.nju.streaming;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StreamingContextState;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @decription: DStream 惰性求值
 * @author: qyl
 */
public class SparkStreamingTest11 {
    public static void main(String[] args) throws InterruptedException {
        // 设置环境配置
        SparkConf conf = new SparkConf ( ).setAppName ("streaming").setMaster ("local[*]");
        // 设置批量处理的周期
        try (JavaStreamingContext ssc = new JavaStreamingContext (conf, Durations.seconds (3))) {

            JavaReceiverInputDStream<String> data9999 = ssc.socketTextStream ("localhost", 9999);

            JavaPairDStream<String, Integer> stream9999 = data9999.mapToPair (data -> new Tuple2<> (data, 8));
            // 惰性求值不会出现时间戳
            stream9999.foreachRDD (rdd -> {
                rdd.mapToPair (r -> new Tuple2<> (r, 1));
            });

            ssc.start ( );
            // 通过检查是否存在stopSpark文件来进行关闭
            new Thread (() -> {
                try (FileSystem fs = FileSystem.get (new URI ("hdfs://linux1:9000"), new Configuration ( ), "root")) {
                    // 优雅地关闭 通过线程来进行监听 进行关闭
                    // 状态可以存在 MYSQL 、 Redis、 Zookeeper 、 HDFS
                    boolean flag = fs.exists (new Path ("hdfs://lunux1:9000/stopSpark"));
                    while (flag) {
                        if (flag) {
                            StreamingContextState state = ssc.getState ( );
                            if (state.equals (StreamingContextState.ACTIVE)) {
                                // 在数据处理完之后，进行关闭
                                ssc.stop (true, true);
                            }
                        }
                        Thread.sleep (5000);
                    }
                    ;
                    ssc.awaitTermination ( );
                } catch (InterruptedException | IOException | URISyntaxException e) {
                    e.printStackTrace ( );
                }
            }).start ( );
        }
    }
}
