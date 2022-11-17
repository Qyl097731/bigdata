package com.nju.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

/**
 * @date:2022/11/16 12:27
 * @author: qyl
 */
public class SparkSqlTest01 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("sql");
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        Dataset<User> df = spark.read().json("data/user.json").as(Encoders.bean(User.class));
        df.show();

        // DataFrame => SQL

        df.createOrReplaceTempView("user");

        spark.sql("select * from user where age = 13").show();
        spark.sql("select username from user").show();
        spark.sql("select avg(age) from user").show();

        // DataFrame => DSL
        df.select("age", "username").show();
        df.selectExpr("username", "age+5 as newName").show();

        // DataSet
        df.filter(df.col("username").equalTo("张三")).show();

    }

    static class User {
        String username;
        Integer age;
    }
}
