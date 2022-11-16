package com.nju.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

/**
 * udf 自定义函数
 *
 * @date:2022/11/16 12:27
 * @author: qyl
 */
public class SparkSqlTest02 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("sql");
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        Dataset<User> df = spark.read().json("data/user.json").as(Encoders.bean(User.class));
        // DataFrame => SQL

        df.createOrReplaceTempView("user");

        spark.udf().register("prefixName", (s) -> "name: " + s, DataTypes.StringType);

        spark.sql("select age,prefixName(username) as name from user where age = 13").show();

    }

    static class User {
        String username;
        Integer age;
    }
}
