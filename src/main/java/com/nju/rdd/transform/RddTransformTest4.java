package com.nju.rdd.transform;

import com.nju.factory.ContextFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @description glom 把多个分区的数据整合成多维数组
 * @date:2022/11/12 15:44
 * @author: qyl
 */
public class RddTransformTest4 {
    public static void main(String[] args) {
        try (JavaSparkContext sc = ContextFactory.create("local")) {
            JavaRDD<Integer> rdd = sc.parallelize(Stream.of(1, 2, 3, 4).collect(Collectors.toList()), 2);
            JavaRDD<List<Integer>> glom = rdd.glom();
            glom.collect().forEach(data -> System.out.println(StringUtils.join(data, ",")));

            // 取出各个分区最大值最后求和
            System.out.println(glom.map(arry -> arry.stream().max(Integer::compareTo).orElseGet(() -> 0)).reduce(Integer::sum));
        }
    }
}
