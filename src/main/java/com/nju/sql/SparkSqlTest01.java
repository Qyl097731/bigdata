package com.nju.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * @date:2022/11/16 12:27
 * @author: qyl
 */
public class SparkSqlTest01 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("sql");
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        // SQL 读取文件
        StructType user = new StructType()
                .add("username", StringType, false)
                .add("age", IntegerType, false);
        Dataset<Row> json = spark.read().schema(user).json("D:\\spark\\data\\user.json");
        json.show();
    }
}
