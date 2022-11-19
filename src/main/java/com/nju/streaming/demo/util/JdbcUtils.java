package com.nju.streaming.demo.util;

import com.alibaba.druid.pool.DruidDataSourceFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

/**
 * @description
 * @date:2022/11/19 20:10
 * @author: qyl
 */
public class JdbcUtils {
    private static DataSource dataSource;

    static {
        try {
            dataSource = init ( );
        } catch (Exception e) {
            e.printStackTrace ( );
        }
    }

    public JdbcUtils() throws Exception {
    }

    //初始化连接池方法
    public static DataSource init() throws Exception {
        Properties properties = new Properties ( );
        properties.setProperty ("driverClassName", "com.mysql.jdbc.Driver");
        properties.setProperty ("url", "jdbc:mysql://linux1:3306/spark-streaming");
        properties.setProperty ("username", "root");
        properties.setProperty ("password", "roort");
        properties.setProperty ("maxActive", "50");
        return DruidDataSourceFactory.createDataSource (properties);
    }

    //获取 MySQL 连接
    public static Connection getConnection() throws SQLException {
        return dataSource.getConnection ( );
    }

    //执行 SQL 语句,单条数据插入
    public static int executeUpdate(Connection connection, String sql, Object[] params) throws SQLException {
        int rtn = 0;
        connection.setAutoCommit (false);

        try (PreparedStatement pstmt = connection.prepareStatement (sql)) {
            if (params != null && params.length > 0) {
                for (int i = 0; i < params.length; i++) {
                    pstmt.setObject (i + 1, params[i]);
                }
            }
            rtn = pstmt.executeUpdate ( );
            connection.commit ( );
        }
        return rtn;
    }

    //执行 SQL 语句,批量数据插入
    public static int[] executeBatchUpdate(Connection connection, String sql, Iterable<Object[]> paramsList) throws SQLException {
        int[] rtn = null;
        connection.setAutoCommit (false);
        try (PreparedStatement pstmt = connection.prepareStatement (sql)) {
            for (Object[] params : paramsList) {
                if (params != null && params.length > 0) {
                    for (int i = 0; i < params.length; i++) {
                        pstmt.setObject (i + 1, params[i]);
                    }
                    pstmt.addBatch ( );
                }
            }
            rtn = pstmt.executeBatch ( );
            connection.commit ( );
        }
        return rtn;
    }

    //判断一条数据是否存在
    public static boolean isExist(Connection connection, String sql, Object[] params) throws SQLException {
        boolean flag = false;
        try (PreparedStatement pstmt = connection.prepareStatement (sql)) {
            for (int i = 0; i < params.length; i++) {
                pstmt.setObject (i + 1, params[i]);
            }
            flag = pstmt.executeQuery ( ).next ( );
        }
        return flag;
    }

    //获取 MySQL 的一条数据
    public static long getDataFromMysql(Connection connection, String sql, Object[] params) throws SQLException {
        long result = 0;
        try (PreparedStatement pstmt = connection.prepareStatement (sql)) {
            for (int i = 0; i < params.length; i++) {
                pstmt.setObject (i + 1, params[i]);
            }
            ResultSet resultSet = pstmt.executeQuery ( );
            while (resultSet.next ( )) {
                result = resultSet.getLong (1);
            }
            resultSet.close ( );
        }
        return result;
    }
}
