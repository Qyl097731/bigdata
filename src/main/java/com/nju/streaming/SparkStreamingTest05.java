package com.nju.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.receiver.Receiver;
import scala.Tuple2;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @decription: DStream 转换 无状态 & 有状态
 * @author: qyl
 */
public class SparkStreamingTest05 {
    public static void main(String[] args) throws InterruptedException {
        // 设置环境配置
        SparkConf conf = new SparkConf ( ).setAppName ("streaming").setMaster ("local[*]");
        // 设置批量处理的周期
        try (JavaStreamingContext ssc = new JavaStreamingContext (conf, Durations.seconds (3))) {
            // 把缓冲区放在哪个文件夹下
            ssc.checkpoint ("checkout");
            JavaReceiverInputDStream<String> data = ssc.socketTextStream ("localhost", 9000);

            JavaPairDStream<String, Integer> pair = data.mapToPair (d -> new Tuple2<> (d, 1));
            // 无状态数据操作
//            pair.reduceByKey (Integer::sum).print ();
            // 有状态数据
            JavaPairDStream<String, Integer> v = pair.updateStateByKey (
                    (Function2<List<Integer>, Optional<Integer>, Optional<Integer>>) (input, buff) -> {
                        java.util.Optional<Integer> sum = input.stream ( ).reduce (Integer::sum);
                        int count = buff.orElse (0) + sum.orElse (0);
                        return Optional.of (count);
                    });
            v.print ( );
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
