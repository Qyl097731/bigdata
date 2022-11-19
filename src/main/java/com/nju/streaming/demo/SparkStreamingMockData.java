package com.nju.streaming.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import scala.Tuple2;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @date:2022/11/19 18:04
 * @author: qyl
 */
public class SparkStreamingMockData {
    public static void main(String[] args) throws InterruptedException {
        Properties prop = new Properties ( );
        // 添加配置
        prop.put (ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "linux1:9092");
        prop.put (ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        prop.put (ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        // 根据配置创建 Kafka 生产者
        try (KafkaProducer<String, String> producer = new KafkaProducer<> (prop)) {
            while (true) {
                Arrays.stream (mockData ( )).forEach (
                        data -> {
                            ProducerRecord<String, String> record = new ProducerRecord<> ("nju", data);
                            producer.send (record);
                        }
                );
                TimeUnit.SECONDS.sleep (3);
            }
        }
    }

    static String[] mockData() {
        Deque<String> array = new ArrayDeque<> ( );
        List<Tuple2<CityInfo, Integer>> CityRandomOpt = Arrays.asList (
                new Tuple2<> (new CityInfo (1, "北京", "华北"), 30),
                new Tuple2<> (new CityInfo (2, "上海", "华东"), 30),
                new Tuple2<> (new CityInfo (3, "广州", "华南"), 10),
                new Tuple2<> (new CityInfo (4, "深圳", "华南"), 20),
                new Tuple2<> (new CityInfo (5, "天津", "华北"), 10)
        );

        Random random = new Random ( );
        // 模拟实时数据：
        // timestamp province city userid adid
        for (int i = 0; i < 50; i++) {
            long timestamp = System.currentTimeMillis ( );
            Tuple2<CityInfo, Integer> tuple2 = CityRandomOpt.get (random.nextInt ( ) % 5);

            CityInfo cityInfo = tuple2._1;
            String city = cityInfo.city;
            String area = cityInfo.aree;
            int adid = 1 + random.nextInt (6);
            int userid = 1 + random.nextInt (6);
            // 拼接实时数据
            array.add (timestamp + " " + area + " " + city + " " + userid + " " + adid);
        }
        return array.toArray (new String[0]);
    }

    static class CityInfo {
        public CityInfo(Integer userId, String city, String aree) {
            this.userId = userId;
            this.city = city;
            this.aree = aree;
        }

        public Integer userId;
        public String city;
        public String aree;
    }
}
