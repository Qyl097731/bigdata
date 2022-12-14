package com.nju.wc;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import static com.nju.consts.UrlConstants.PATH;


/**
 * @date:2022/11/1 23:29
 * @author: qyl
 */
public class JavaWordCount {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("JavaWordCount").master("local").getOrCreate();

        JavaRDD<String> lines = spark.read().textFile(PATH).javaRDD();

        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());

        JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

        JavaPairRDD<String, Integer> counts = ones.reduceByKey(Integer::sum);

        List<Tuple2<String, Integer>> output = counts.collect();

        for (Tuple2<?, ?> tuple2 : output) {
            System.out.println(tuple2._1() + ": " + tuple2._2());
        }
        spark.stop();
    }
}
