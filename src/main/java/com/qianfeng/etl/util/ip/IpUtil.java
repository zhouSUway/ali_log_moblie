package com.qianfeng.etl.util.ip;

import com.github.jarod.qqwry.IPZone;
import com.github.jarod.qqwry.QQWry;

import java.io.IOException;

public class IpUtil {

    public static void parserIp(String ip){

        try {
            QQWry qqwry = new QQWry();
            IPZone zooe = qqwry.findIP(ip);
            System.out.println(zooe.getMainInfo());
        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
