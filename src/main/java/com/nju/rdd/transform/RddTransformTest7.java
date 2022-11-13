package com.nju.rdd.transform;

import com.nju.factory.ContextFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @description sample抽取数据
 * @date:2022/11/12 15:44
 * @author: qyl
 */
public class RddTransformTest7 {
    public static void main(String[] args) {
        try (JavaSparkContext sc = ContextFactory.create("local")) {
            JavaRDD<Integer> rdd =
                    sc.parallelize(IntStream.rangeClosed(1, 10).boxed().collect(Collectors.toList()),
                            2);


            System.out.println(StringUtils.join(rdd.sample(
                    false, // 是否之后放回 false对应第二个参数允许被抽取出来的数概率阈值，true可以被抽取出来几次
                    0.4        // 每条数据被抽取的概率
                    // 随机数种子 ，如果不设置默认是系统时间
            ).collect(), ","));
        }
    }
}
