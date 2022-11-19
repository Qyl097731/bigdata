package com.nju.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.receiver.Receiver;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @decription: 数据源DIY
 * @date:2022/11/17 23:59
 * @author: qyl
 */
public class SparkStreamingTest03 {
    public static void main(String[] args) throws InterruptedException {
        // 设置环境配置
        SparkConf conf = new SparkConf ( ).setAppName ("streaming").setMaster ("local[*]");
        // 设置批量处理的周期
        try (JavaStreamingContext ssc = new JavaStreamingContext (conf, Durations.seconds (3))) {
            JavaReceiverInputDStream<String> dStream = ssc.receiverStream (new MyReceiver (StorageLevel.MEMORY_ONLY ( )));
            dStream.print ( );

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
