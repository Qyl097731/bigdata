package com.nju.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.receiver.Receiver;
import scala.Tuple2;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @decription: transform 能实现所有得rdd功能
 * @author: qyl
 */
public class SparkStreamingTest07 {
    public static void main(String[] args) throws InterruptedException {
        // 设置环境配置
        SparkConf conf = new SparkConf ( ).setAppName ("streaming").setMaster ("local[*]");
        // 设置批量处理的周期
        try (JavaStreamingContext ssc = new JavaStreamingContext (conf, Durations.seconds (5))) {

            JavaReceiverInputDStream<String> data8888 = ssc.socketTextStream ("localhost", 8888);
            JavaReceiverInputDStream<String> data9999 = ssc.socketTextStream ("localhost", 9999);

            JavaPairDStream<String, Integer> stream8888 = data8888.mapToPair (data -> new Tuple2<> (data, 9));
            JavaPairDStream<String, Integer> stream9999 = data9999.mapToPair (data -> new Tuple2<> (data, 8));

            JavaPairDStream<String, Tuple2<Integer, Integer>> join = stream8888.join (stream9999);
            join.print ( );

            ssc.start ( );
            ssc.awaitTermination ( );
        }
    }

    static class MyReceiver extends Receiver<String> {

        private boolean flag = true;

        public MyReceiver(StorageLevel storageLevel) {
            super (storageLevel);
        }

        @Override
        public void onStart() {
            new Thread (() -> {
                while (flag) {
                    String message = "采集的数据为：" + new Random ( ).nextInt (10);
                    // 数据封装
                    store (message);
                    try {
                        TimeUnit.MILLISECONDS.sleep (500);
                    } catch (InterruptedException e) {
                        e.printStackTrace ( );
                    }
                }
            }).start ( );
        }

        @Override
        public void onStop() {
            flag = false;
        }
    }
}
