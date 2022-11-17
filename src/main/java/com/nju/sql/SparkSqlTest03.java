package com.nju.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Aggregator;

/**
 * @description UDAF 强类型Aggregate
 * @date:2022/11/17 16:34
 * @author: qyl
 */
public class SparkSqlTest03 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("sql").setMaster("local");
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        Dataset<User> df = spark.read().json("data/user.json").as(Encoders.bean(User.class));
        df.createOrReplaceTempView("user");

        TypedColumn<User, Long> userLongTypedColumn = new MyAvgAgeUdaf().toColumn();
        df.select(userLongTypedColumn).show();
    }

    static class User {
        String username;
        Long age;

        public User(String username, Long age) {
            this.username = username;
            this.age = age;
        }
    }


    static class Buff {
        Long sum;
        Long cnt;

        public Buff(Long sum, Long cnt) {
            this.sum = sum;
            this.cnt = cnt;
        }
    }

    static class MyAvgAgeUdaf extends Aggregator<User, Buff, Long> {
        // 初始值 缓冲区初始化
        @Override
        public Buff zero() {
            return new Buff(0L, 0L);
        }

        @Override
        public Buff reduce(Buff b, User a) {
            b.sum += a.age;
            b.cnt += 1;
            return b;
        }

        @Override
        public Buff merge(Buff b1, Buff b2) {
            b1.cnt += b2.cnt;
            b1.sum += b2.sum;
            return b1;
        }

        @Override
        public Long finish(Buff reduction) {
            return reduction.sum / reduction.cnt;
        }

        @Override
        public Encoder<Buff> bufferEncoder() {
            return Encoders.bean(Buff.class);
        }

        @Override
        public Encoder<Long> outputEncoder() {
            return Encoders.LONG();
        }
    }

}
