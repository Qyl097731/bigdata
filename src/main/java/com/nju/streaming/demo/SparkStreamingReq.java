package com.nju.streaming.demo;

import com.nju.streaming.demo.util.JdbcUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @decription: kafka
 * @date:2022/11/17 23:59
 * @author: qyl
 */
public class SparkStreamingReq {
    public static void main(String[] args) throws InterruptedException, SQLException {
        // 设置环境配置
        SparkConf conf = new SparkConf ( ).setAppName ("streaming").setMaster ("local[*]");
        // 设置批量处理的周期
        try (JavaStreamingContext ssc = new JavaStreamingContext (conf, Durations.seconds (3))) {
            Map<String, Object> kafkaPara = new HashMap<> ( );
            kafkaPara.put (ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    "linux1:9092,linux2:9092,linux3:9092");
            kafkaPara.put (ConsumerConfig.GROUP_ID_CONFIG, "atguigu");
            JavaInputDStream<ConsumerRecord<String, String>> kafkaDataDS = KafkaUtils.createDirectStream (ssc,
                    LocationStrategies.PreferConsistent ( ),
                    ConsumerStrategies.Subscribe (Collections.singletonList ("nju"), kafkaPara));
            JavaDStream<AdClickData> adClickData = kafkaDataDS.map (kafkaData -> {
                String[] value = kafkaData.value ( ).split (" ");
                return new AdClickData (value[0], value[1], value[2], value[3], value[4]);
            });

            // TODO 周期性获取黑名单数据
            // TODO 判断用户是否在黑名单种
            // TODO 如果不在黑名单中，那么进行统计数量
            // 通过JDBC周期性获取黑名单
            Deque<String> blackList = new ArrayDeque<> ( );
            Connection conn = JdbcUtils.getConnection ( );
            try (PreparedStatement preparedStatement = conn.prepareStatement ("select userid from black_list")) {

                ResultSet resultSet = preparedStatement.executeQuery ( );
                while (resultSet.next ( )) {
                    blackList.add (resultSet.getString (1));
                }
                resultSet.close ( );
            }
            conn.close ( );
            JavaDStream<Tuple4> stream = adClickData.transform (
                    (Function<JavaRDD<AdClickData>, JavaRDD<Tuple4>>) rdd -> {
                        rdd.filter (data -> !blackList.contains (data.user));

                        JavaPairRDD<Tuple3<String, String, String>, Integer> reduceByKey = rdd.mapToPair (data -> {
                            SimpleDateFormat sdf = new SimpleDateFormat ("yyyyMMdd");
                            String day =
                                    sdf.format (new Date (Long.parseLong (data.ts)));
                            String user = data.user;
                            String ad = data.ad;
                            return new Tuple2<> (new Tuple3<> (day, user, ad), 1);
                        }).reduceByKey (Integer::sum);
                        return reduceByKey.map (r -> new Tuple4<> (r._1._1 ( ), r._1._2 ( ), r._1._3 ( ), r._2));
                    });
            // TODO 如果统计数量超过阈值，将用户拉入黑名单
            // TODO 如果没有超过阈值，就把当天广告点击数量进行更新
            // TODO 判断更新后的点击数量是否超过阈值，那么将用户拉入黑名单
            ssc.start ( );
            ssc.awaitTermination ( );
        }
    }


}
