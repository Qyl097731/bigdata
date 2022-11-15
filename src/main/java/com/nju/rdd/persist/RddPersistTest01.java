package com.nju.rdd.persist;

import com.nju.factory.ContextFactory;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @date:2022/11/15 17:39
 * @author: qyl
 */
public class RddPersistTest01 {
    public static void main(String[] args) {
        try (JavaSparkContext sc = ContextFactory.create("local")) {
            JavaRDD<String> rdd = sc.parallelize(Arrays.asList("Hello Scala", "Hello Spark"));
            rdd = rdd.flatMap(r -> {
                System.out.println("#######");
                return Arrays.asList(r.split(" ")).iterator();
            });
            // 存到内存 避免重复计算 删除cache就会导致重复计算
//            rdd.cache();
            // 存到文件 任务结束就删除
//            rdd.persist(StorageLevel.DISK_ONLY());
            JavaPairRDD<String, Integer> pair = rdd.mapToPair(r -> new Tuple2<>(r, 1));
            System.out.println("@@@@@@@@@@@");
            pair.reduceByKey(Integer::sum).collect().forEach(System.out::println);
            System.out.println("@@@@@@@@@@@");
            pair.groupByKey().collect().forEach(System.out::println);

        }
    }
}
