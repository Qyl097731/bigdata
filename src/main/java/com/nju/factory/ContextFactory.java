package com.nju.factory;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @description
 * @date:2022/11/12 15:31
 * @author: qyl
 */
public class ContextFactory {
    static SparkConf conf = null;
    static JavaSparkContext ctx = null;
    private static final String LOCAL = "local";
    private static final String CLUSTER = "cluster";
    private static final String STANDALONE = "standalone";

    private ContextFactory() {
    }

    public static JavaSparkContext create(String master) {
        if (LOCAL.equals(master)) {
            return createMaster();
        } else if (CLUSTER.equals(master)) {
            return createCluster();
        } else if (STANDALONE.equals(master)) {
            return createStandalone();
        } else {
            throw new IllegalArgumentException();
        }
    }

    private static JavaSparkContext createMaster() {
        conf = new SparkConf().setMaster(LOCAL + "[*]").setAppName(LOCAL);
        ctx = new JavaSparkContext(conf);
        return ctx;
    }

    private static JavaSparkContext createCluster() {
        conf = new SparkConf().setMaster(CLUSTER).setAppName(LOCAL);
        ctx = new JavaSparkContext(conf);
        return ctx;
    }

    private static JavaSparkContext createStandalone() {
        conf = new SparkConf().setMaster(STANDALONE).setAppName(LOCAL);
        ctx = new JavaSparkContext(conf);
        return ctx;
    }
}
