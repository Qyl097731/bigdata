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
public class SparkStreamingTest06 {
    public static void main(String[] args) throws InterruptedException {
        // 设置环境配置
        SparkConf conf = new SparkConf ( ).setAppName ("streaming").setMaster ("local[*]");
        // 设置批量处理的周期
        try (JavaStreamingContext ssc = new JavaStreamingContext (conf, Durations.seconds (3))) {

            JavaReceiverInputDStream<String> lines = ssc.socketTextStream ("localhost", 9999);

            // transform 方法可以获取底层rdd，之后在进行实现
            // 场景 ： 1.DStream功能不完善、 2.需要周期执行任务
            // CODE : Driver
            lines.transform (
                    rdd ->
                            // CODE : DRIVER 周期性执行
                            rdd.map (
                                    // CODE :Executor
                                    str -> new Tuple2<> (str, 1)
                            )
            );


            // CODE : Driver
            JavaPairDStream<String, Integer> rdd = lines.mapToPair (
                    data -> {
                        // CODE : Executor
                        return new Tuple2<> (data, 1);
                    }
            );

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
