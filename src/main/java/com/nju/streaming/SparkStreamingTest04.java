package com.nju.streaming;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @decription: kafka
 * @date:2022/11/17 23:59
 * @author: qyl
 */
public class SparkStreamingTest04 {
    public static void main(String[] args) throws InterruptedException {
        // 设置环境配置
        SparkConf conf = new SparkConf ( ).setAppName ("streaming").setMaster ("local[*]");
        // 设置批量处理的周期
        try (JavaStreamingContext ssc = new JavaStreamingContext (conf, Durations.seconds (3))) {
            Map<String, Object> kafkaPara = new HashMap<> ( );
            kafkaPara.put (ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    "linux1:9092,linux2:9092,linux3:9092");
            kafkaPara.put (ConsumerConfig.GROUP_ID_CONFIG, "atguigu");
            JavaInputDStream<ConsumerRecord<Object, Object>> kafkaDataDS = KafkaUtils.createDirectStream (ssc, LocationStrategies.PreferConsistent ( ),
                    ConsumerStrategies.Subscribe (Collections.singletonList ("nju"), kafkaPara));
            kafkaDataDS.map (ConsumerRecord::value).print ( );
            ssc.start ( );
            ssc.awaitTermination ( );
        }
    }
}
