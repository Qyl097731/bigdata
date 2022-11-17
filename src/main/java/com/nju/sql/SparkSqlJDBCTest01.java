package com.nju.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * @description
 * @date:2022/11/17 22:35
 * @author: qyl
 */
public class SparkSqlJDBCTest01 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("jdbc");
        try (SparkSession spark = SparkSession.builder().config(conf).getOrCreate()) {

            Dataset<Row> df = spark.read().format("jdbc")
                    .option("url", "jdbc:mysql://linux:3306/spark-sql")
                    .option("driver", "com.mysql.jdbc.Driver")
                    .option("user", "root")
                    .option("password", "123123")
                    .option("dbtable", "user")
                    .load();

            // 保存数据
            df.write()
                    .format("jdbc")
                    .option("url", "jdbc:mysql://linux:3306/spark-sql")
                    .option("driver", "com.mysql.jdbc.Driver")
                    .option("user", "root")
                    .option("password", "123123")
                    .option("dbtable", "user")
                    .mode(SaveMode.Append)
                    .save();
        }
    }
}
