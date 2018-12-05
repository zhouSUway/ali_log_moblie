package com.qianfeng.util;

import com.qianfeng.common.GlobalConstants;

import java.sql.*;

public class JdbcUtil {

    //静态加载驱动

    static {
        try {
            Class.forName(GlobalConstants.DRIVER);

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
    public static Connection getConn(){
        Connection conn= null;
        try {
            conn=DriverManager.getConnection(GlobalConstants.URL,GlobalConstants.USERNAME,GlobalConstants.PASSWORD);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return conn;

    }


    public static void close(Connection conn, PreparedStatement psm, ResultSet rs) throws SQLException {
        if (conn!=null){
            conn.close();
        }
        if (psm!=null){
            psm.close();
        }
        if (rs!=null)
        {
            rs.close();
        }

    }
}
