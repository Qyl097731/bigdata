package com.nju.streaming;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StreamingContext;

/**
 * @decription: 恢复数据
 * @author: qyl
 */
public class SparkStreamingTest12 {
    public static void main(String[] args) {
        // 通过检查点来恢复数据,否则在重新启动之后之前的数据就丢失了
        StreamingContext ssc = StreamingContext.getActiveOrCreate ("cp", () -> {
            // 设置环境配置
            SparkConf conf = new SparkConf ( ).setAppName ("streaming").setMaster ("local[*]");
            // 设置批量处理的周期
            return new StreamingContext (conf, Durations.seconds (3));

        }, new Configuration ( ), true);
        ssc.checkpoint ("cp");
        ssc.start ( );
        ssc.awaitTermination ( );
    }
}
